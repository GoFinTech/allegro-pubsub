<?php

/*
 * This file is part of the Allegro framework.
 *
 * (c) 2019,2020 Go Financial Technologies, JSC
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace GoFinTech\Allegro\PubSub;

use DateTime;
use Exception;
use Google\Cloud\Datastore\DatastoreClient;
use Google\Cloud\PubSub\Message;
use Psr\Log\LoggerInterface;
use RuntimeException;
use Symfony\Component\Serializer\SerializerInterface;
use Symfony\Component\Validator\Validator\ValidatorInterface;

abstract class PubSubHandler
{
    /** @var PubSubRequest */
    protected $request;
    /** @var LoggerInterface */
    protected $log;


    protected abstract function handleMessage($msg);

    /**
     * @param PubSubRequest $request
     */
    public function processRequest(PubSubRequest $request)
    {
        $this->request = $request;
        $this->log = $request->getLogger();

        $typeInfo = $request->getMessageTypeInfo();

        try {

            if ($typeInfo->messageClass() == ':none:') {
                $data = json_decode($request->getMessage()->data());
                if (is_null($data)) {
                    // Errors during message parsing are irrecoverable
                    $request->acknowledge();
                    throw new RuntimeException("Message body is not a valid JSON: " . json_last_error_msg());
                }
            }
            else {
                /** @var SerializerInterface $serializer */
                $serializer = $request->getContainer()->get('serializer');
                /** @var ValidatorInterface $validator */
                $validator = $request->getContainer()->get('validator');
                try {
                    $data = $serializer->deserialize($request->getMessage()->data(),
                        $typeInfo->messageClass(), 'json');
                }
                catch (Exception $e) {
                    // Errors during message parsing are irrecoverable
                    $request->acknowledge();
                    throw $e;
                }
                $errors = $validator->validate($data);
                if (count($errors) > 0) {
                    // Validations errors are irrecoverable
                    $request->acknowledge();
                    throw new RuntimeException("Message validation failed:\n$errors");
                }
            }

            $this->handleMessage($data);

            $request->acknowledge();

        } catch (Exception $ex) {
            if (get_class($ex) == 'PDOException' && $ex->getCode() == 'HY000') {
                // PostgreSQL PDO: SQLSTATE[HY000]: General error: 7 no connection to the server
                $this->log->error("DB connection fail detected", ['exception' => $ex]);
                throw $ex;
            }
            $this->handleException($ex);
            sleep(5);
            return;
        }
    }

    protected function handleException(Exception $ex)
    {
        $subscriptionName = $this->request->getSubscriptionName();
        $lastSlash = strrpos($subscriptionName, '/');
        if ($lastSlash !== false) {
            $subscriptionName = substr($subscriptionName, $lastSlash + 1);
        }

        /** @var Message $message */
        $message = $this->request->getMessage();

        /** @var DatastoreClient $datastore */
        $datastore = $this->request->getContainer()->has(DatastoreClient::class)
            ? $this->request->getContainer()->get(DatastoreClient::class) : new DatastoreClient();

        $key = $datastore->key('FailedMessages', "{$message->id()}:$subscriptionName");

        // We don't care for exact number of retries, so we don't use transaction here.
        $entity = $datastore->lookup($key);

        $now = new DateTime();

        if (!is_null($entity)) {
            $entity['tries'] = $entity['tries'] + 1;
            $entity['model_time'] = $now;
            $datastore->update($entity);
        } else {
            $entity = $datastore->entity($key, [
                'error' => $this->formatException($ex),
                'error_trace' => $ex->getTraceAsString(),
                'subscription' => $subscriptionName,
                'message_id' => $message->id(),
                'body' => $message->data(),
                'message_type' => $message->attribute('message-type'),
                'tries' => 1,
                'created_at' => $now,
                'model_version' => 2,
                'model_time' => $now,
            ]);
            $entity->setExcludeFromIndexes(['error', 'error_trace', 'body']);
            $datastore->insert($entity);
        }

        if ($entity['tries'] >= 3) {
            $this->request->acknowledge();
        }

        $this->log->error("Failed to process message {$message->id()}", ['exception' => $ex]);
    }

    protected function formatException(Exception $ex): string
    {
        $className = get_class($ex);
        return "{$ex->getFile()}:{$ex->getLine()} $className: {$ex->getMessage()}";
    }
}
