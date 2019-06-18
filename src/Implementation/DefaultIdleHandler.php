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


use GoFinTech\Allegro\PubSub\IdleHandler;

class DefaultIdleHandler implements IdleHandler
{
    public function idleAction()
    {
        return;
    }
}
