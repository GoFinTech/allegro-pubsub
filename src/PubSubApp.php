<?php

/*
 * This file is part of the Allegro framework.
 *
 * (c) 2019,2021 Go Financial Technologies, JSC
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace GoFinTech\Allegro\PubSub;

use GoFinTech\Allegro\AllegroApp;
use GoFinTech\Allegro\PubSub\Implementation\DefaultIdleHandler;
use GoFinTech\Allegro\PubSub\Implementation\DefaultMessageHandler;
use GoFinTech\Allegro\PubSub\Implementation\MessageTypeInfo;
use Google\Cloud\PubSub\PubSubClient;
use Google\Cloud\PubSub\Subscription;
use LogicException;
use Psr\Log\LoggerAwareInterface;
use Psr\Log\LoggerInterface;
use Symfony\Component\Config\FileLocator;
use Symfony\Component\Yaml\Yaml;

class PubSubApp implements LoggerAwareInterface
{
    /** @var AllegroApp */
    private $app;
    /** @var LoggerInterface */
    private $log;
    /** @var string */
    private $appName;

    /** @var string */
    private $subscriptionName;
    /** @var MessageTypeInfo[] indexed by messageType */
    private $messageTypes;
    /** @var string */
    private $idleHandlerService;

    /**
     * PubSubApp constructor.
     * @param string|AllegroApp $configSection config section name in pubsub.yml.
     *      Might be an AllegroApp instance for backward compatibility.
     * @param string|null $legacyConfigSection config section name if app instance is passed as the first argument
     */
    public function __construct($configSection, $legacyConfigSection = null)
    {
        $this->app = AllegroApp::resolveConstructorParameters("PubSubApp", $configSection, $legacyConfigSection);
        $this->log = $this->app->getLogger();

        $this->loadConfiguration($this->app->getConfigLocator(), $configSection);
    }

    /**
     * Shorthand for instantiating a PubSubApp with specified config and calling run().
     * @param string $configSection
     */
    public static function exec(string $configSection): void
    {
        $app = new PubSubApp($configSection);
        $app->run();
    }

    /**
     * Overrides default logger that is received from Allegro.
     *
     * @param LoggerInterface $logger
     * @return void
     */
    public function setLogger(LoggerInterface $logger)
    {
        $this->log = $logger;
    }

    private function loadConfiguration(FileLocator $locator, string $configSection): void
    {
        $config = Yaml::parseFile($locator->locate('pubsub.yml'));
        $this->appName = $configSection;

        $pubsub = $config[$configSection];
        $this->subscriptionName = $pubsub['subscription'];
        $this->idleHandlerService = $pubsub['idleHandler'] ?? null;

        foreach ($pubsub['handlers'] as $handler) {
            $handlerService = $handler['service'];
            $messageClass = $handler['messageClass'] ?? null;

            if ($messageClass) {
                /** @var MessageInterface $messageClass */
                $messageType = $messageClass::getMessageType();
                $this->messageTypes[$messageType] = new MessageTypeInfo($messageType, $messageClass, $handlerService);
            }
            else {
                $this->messageTypes[':default:'] =
                    new MessageTypeInfo(':default:', ':none:', $handlerService);
            }
        }

        if (!$this->messageTypes)
            throw new LogicException("PubSubApp: no message handlers defined");

        if (!isset($this->messageTypes[':default:'])) {
            $this->messageTypes[':default:'] = new MessageTypeInfo(':default:', ':none:', ':default:');
        }
    }

    public function run()
    {
        $this->app->compile();
        $container = $this->app->getContainer();

        /** @var PubSubClient $pubsub */
        $pubsub = $container->has(PubSubClient::class) ? $container->get(PubSubClient::class)
            : new PubSubClient();

        /** @var Subscription $subscription */
        $subscription = $pubsub->subscription($this->subscriptionName);

        /** @var IdleHandler $idleHandler */
        $idleHandler = isset($this->idleHandlerService) ?
            $container->get($this->idleHandlerService) : new DefaultIdleHandler();

        $this->log->notice("Allegro Pub/Sub: {$this->appName} starts polling {$this->subscriptionName}");

        while (true) {
            if ($this->app->isTermSignalReceived()) {
                $this->log->info("Performing graceful shutdown on SIGTERM");
                break;
            }

            $messages = $subscription->pull([
                'returnImmediately' => true,
                'maxMessages' => 1,
            ]);

            if (!count($messages)) {
                $this->app->ping();
                if ($idleHandler->idleAction()) {
                   continue;
                }
                sleep(3);
                continue;
            }

            foreach ($messages as $message) {
                $this->log->info("Processing message {$message->id()}");

                $messageType = $message->attribute('message-type');
                $typeMap = $this->messageTypes[$messageType] ?? $this->messageTypes[':default:'];
                $handlerName = $typeMap->handlerName();

                /** @var PubSubHandler $handler */
                $handler = ($handlerName != ':default:')
                    ? $container->get($typeMap->handlerName()) : new DefaultMessageHandler();

                $request = new PubSubRequest($subscription, $message, $typeMap, $this->log, $container);

                $handler->processRequest($request);

                $this->log->info("Finished processing message {$message->id()}");
                $this->app->ping();
            }
        }
    }
}
