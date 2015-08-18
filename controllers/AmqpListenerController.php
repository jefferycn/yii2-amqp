<?php
/**
 * @link https://github.com/webtoucher/yii2-amqp
 * @copyright Copyright (c) 2014 webtoucher
 * @license https://github.com/webtoucher/yii2-amqp/blob/master/LICENSE.md
 */

namespace webtoucher\amqp\controllers;

use yii\console\Exception;
use yii\helpers\Inflector;
use yii\helpers\Json;
use PhpAmqpLib\Message\AMQPMessage;
use webtoucher\amqp\components\Amqp;
use webtoucher\amqp\components\AmqpInterpreter;
use webtoucher\amqp\components\AmpqInterpreterInterface;
use webtoucher\commands\Controller;
use Yii;

/**
 * AMQP listener controller.
 *
 * @author Alexey Kuznetsov <mirakuru@webtoucher.ru>
 * @since 2.0
 */
class AmqpListenerController extends AmqpConsoleController
{
    /**
     * Interpreter classes for AMQP messages. This class will be used if interpreter class not set for exchange.
     *
     * @var array
     */
    public $interpreters = [];

    public function actionRun($routingKey = '#', $type = Amqp::TYPE_TOPIC)
    {
        if (isset($this->interpreters[$this->exchange])) {
            $config = $this->interpreters[$this->exchange];
            if (is_array($config)) {
                // class is not option for 
                unset($config['class']);
                Yii::configure($this->amqp, $config);
            }
        }
        $this->amqp->listen($this->exchange, $routingKey, [$this, 'callback'], $type);
    }

    public function callback(AMQPMessage $msg)
    {
        $routingKey = $msg->delivery_info['routing_key'];
        $method = 'read' . Inflector::camelize($routingKey);

        if (!isset($this->interpreters[$this->exchange])) {
            $interpreter = $this;
        } elseif (is_array($this->interpreters[$this->exchange]) && !empty($this->interpreters[$this->exchange]['class'])) {
            $class = $this->interpreters[$this->exchange]['class'];
        } else {
            $class = $this->interpreters[$this->exchange];
        }

        if (class_exists($class)) {
            $interpreter = new $class;
            if (!$interpreter instanceof AmqpInterpreter) {
                throw new Exception(sprintf("Class '%s' is not correct interpreter class.", $class));
            }
        } else {
            throw new Exception(sprintf("Interpreter class '%s' was not found.", $class));
        }

        if (method_exists($interpreter, $method)) {
            $interpreter->$method(Json::decode($msg->body, true), $msg);
        } else {
            if (!isset($this->interpreters[$this->exchange])) {
                $interpreter = new AmqpInterpreter();
            }
            $interpreter->log(
                sprintf("Unknown routing key '%s' for exchange '%s'.", $routingKey, $this->exchange),
                $interpreter::MESSAGE_ERROR
            );
            // debug the message
            $interpreter->log(
                print_r(Json::decode($msg->body, true), true),
                $interpreter::MESSAGE_INFO
            );
        }
    }
}
