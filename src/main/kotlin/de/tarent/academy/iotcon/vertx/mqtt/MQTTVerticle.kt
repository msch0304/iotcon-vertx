package de.tarent.academy.iotcon.vertx.mqtt

import io.netty.handler.codec.mqtt.MqttQoS
import io.vertx.core.AbstractVerticle
import io.vertx.core.AsyncResult
import io.vertx.core.Future
import io.vertx.core.buffer.Buffer
import io.vertx.core.eventbus.Message
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import io.vertx.mqtt.MqttClient
import io.vertx.mqtt.MqttClientOptions


class MQTTVerticle: AbstractVerticle() {

    override fun start(startFuture: Future<Void>?) {
        startFuture?.handle(startMQTTClient())
    }

    fun startMQTTClient():Future<Void> {
        val future = Future.future<Void>()

        val mqttOptions = MqttClientOptions()
        mqttOptions.clientId = "iotcon-vertx"
        mqttOptions.setAutoKeepAlive(true)
        val mqttClient = MqttClient.create(vertx, mqttOptions)
        mqttClient.connect(config().getInteger("broker_port"), config().getString("broker_uri")){ connack ->
            if (connack.succeeded()){
                future.handle(subscribeToTopics(mqttClient))
            }else{
                future.fail(connack.cause())
            }
        }
        return future
    }

    fun subscribeToTopics(mqttClient: MqttClient):Future<Void> {
        val future = Future.future<Void>()
        val topic = config().getString("topic_subscribe")
        val eb = vertx.eventBus()
        mqttClient.publishHandler(){ message ->
            val mtopic = message.topicName()
            if (mtopic.contains("create")){
                eb.send("books.post", message.payload().toJsonObject()){msg: AsyncResult<Message<JsonObject>> ->
                    if (msg.succeeded()){
                        mqttClient.publish(config().getString("topic_publish"), Buffer.buffer(msg.result().body().encodePrettily()), MqttQoS.AT_MOST_ONCE, false, false)
                    }else{
                        mqttClient.publish(config().getString("topic_publish"), Buffer.buffer(msg.cause().message), MqttQoS.AT_MOST_ONCE, false, false)

                    }
                }
            }
            if (mtopic.contains("update")){
                eb.send("books.put", message.payload().toJsonObject()){msg: AsyncResult<Message<JsonObject>> ->
                    if (msg.succeeded()){
                        mqttClient.publish(config().getString("topic_publish"), Buffer.buffer(msg.result().body().encodePrettily()), MqttQoS.AT_MOST_ONCE, false, false)
                    }else{
                        mqttClient.publish(config().getString("topic_publish"), Buffer.buffer(msg.cause().message), MqttQoS.AT_MOST_ONCE, false, false)

                    }
                }
            }
            if (mtopic.contains("list")){
                eb.send("books.get", ""){msg: AsyncResult<Message<JsonArray>> ->
                    if (msg.succeeded()){
                        mqttClient.publish(config().getString("topic_publish"), Buffer.buffer(msg.result().body().encodePrettily()), MqttQoS.AT_MOST_ONCE, false, false)
                    }else{
                        mqttClient.publish(config().getString("topic_publish"), Buffer.buffer(msg.cause().message), MqttQoS.AT_MOST_ONCE, false, false)

                    }
                }
            }
            if (mtopic.contains("delete")){
                eb.send("books.delete", Integer.parseInt(message.payload().toString())){msg: AsyncResult<Message<String>> ->
                    if (msg.succeeded()){
                        mqttClient.publish(config().getString("topic_publish"), Buffer.buffer(msg.result().body()), MqttQoS.AT_MOST_ONCE, false, false)
                    }else{
                        mqttClient.publish(config().getString("topic_publish"), Buffer.buffer(msg.cause().message), MqttQoS.AT_MOST_ONCE, false, false)

                    }
                }
            }

        }.subscribeCompletionHandler(){
                println("subcribed"+ it.grantedQoSLevels().toString())
                future.complete()
            }.subscribe(topic, MqttQoS.AT_LEAST_ONCE.value())

        return future
    }
}