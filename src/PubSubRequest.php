<?php

/*
 * This file is part of the Allegro framework.
 *
 * (c) 2019 Go Financial Technologies, JSC
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace GoFinTech\Allegro\PubSub;

use GoFinTech\Allegro\PubSub\Implementation\MessageTypeInfo;
use Google\Cloud\PubSub\Message;
use Google\Cloud\PubSub\Subscription;
use Psr\Container\ContainerInterface;
use Psr\Log\LoggerInterface;

class PubSubRequest
{
    /** @var Subscription */
    private $subscription;
    /** @var Message */
    private $message;
    /** @var MessageTypeInfo */
    private $messageTypeInfo;
    /** @var LoggerInterface */
    private $log;
    /** @var ContainerInterface */
    private $container;

    /** @var bool */
    private $acknowledged;
    /** @var bool */
    private $wantsRetry;

    public function __construct(Subscription $subscription,
                                Message $message,
                                MessageTypeInfo $typeInfo,
                                LoggerInterface $logger,
                                ContainerInterface $container)
    {
        $this->subscription = $subscription;
        $this->message = $message;
        $this->messageTypeInfo = $typeInfo;
        $this->log = $logger;
        $this->container = $container;
    }

    public function getSubscriptionName(): string
    {
        return $this->subscription->name();
    }

    public function getMessage(): Message
    {
        return $this->message;
    }

    public function getMessageTypeInfo() : MessageTypeInfo
    {
        return $this->messageTypeInfo;
    }

    public function getContainer(): ContainerInterface
    {
        return $this->container;
    }

    public function getLogger(): LoggerInterface
    {
        return $this->log;
    }

    /**
     * Acknowledges received message.
     * Message is removed from subscription immediately
     * and will not be re-delivered even if this request fails.
     */
    public function acknowledge()
    {
        if ($this->acknowledged)
            return;
        if ($this->wantsRetry) {
            $this->log->info("Ignoring message acknowledge for retry {$this->message->id()}");
            return;
        }
        $this->subscription->acknowledge($this->message);
        $this->acknowledged = true;
        $this->log->info("Acknowledged message {$this->message->id()}");
    }

    /**
     * Prevents message auto-acknowledge on successful processing.
     * Currently, it just makes further calls to acknowledge() to be ignored.
     */
    public function retry()
    {
        if ($this->acknowledged)
            $this->log->error("PubSubRequest::retry() called after acknowledge()");
        $this->wantsRetry = true;
    }
}
