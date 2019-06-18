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


interface IdleHandler
{
    /**
     * Allows to do some useful work when message loop is idle.
     * @return TRUE if default sleep should not be invoked
     */
    public function idleAction();
}
