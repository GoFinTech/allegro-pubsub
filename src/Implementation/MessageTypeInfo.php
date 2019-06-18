<?php

/*
 * This file is part of the Allegro framework.
 *
 * (c) 2019 Go Financial Technologies, JSC
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace GoFinTech\Allegro\PubSub\Implementation;


class MessageTypeInfo
{
    /** @var string */
    private $messageType;
    /** @var string */
    private $messageClass;
    /** @var string */
    private $handlerName;

    public function __construct(string $messageType, string $messageClass, string $handlerName)
    {
        $this->messageType = $messageType;
        $this->messageClass = $messageClass;
        $this->handlerName = $handlerName;
    }

    public function messageType(): string
    {
        return $this->messageType;
    }

    public function messageClass(): string
    {
        return $this->messageClass;
    }

    public function handlerName(): string
    {
        return $this->handlerName;
    }

}
