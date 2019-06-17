package de.tarent.academy.iotcon.vertx.mqtt

import io.netty.handler.codec.http.HttpResponseStatus
import io.vertx.config.ConfigRetriever
import io.vertx.core.*
import io.vertx.core.eventbus.Message
import io.vertx.core.http.HttpHeaders
import io.vertx.core.http.HttpMethod
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import io.vertx.ext.web.Router
import io.vertx.ext.web.handler.BodyHandler
import io.vertx.kotlin.ext.web.api.contract.RouterFactoryOptions


class MainVerticle : AbstractVerticle() {

    override fun start(startFuture: Future<Void>) {

        var router = defineRouter()
        deployVerticles()
        vertx
            .createHttpServer()
            .requestHandler(router)
            .listen(8888) { http ->
                if (http.succeeded()) {
                    startFuture.complete()
                    println("HTTP server started on port 8888")
                } else {
                    startFuture.fail(http.cause());
                }
            }
    }

    fun defineRouter(): Router {
        var router = Router.router(vertx)
        router.route().handler(BodyHandler.create());
        val eb = vertx.eventBus()

        val create = router.route(HttpMethod.POST, "/books")
        create.handler() { rt ->

            val json = rt.bodyAsJson

            val response = rt.response()
            response.putHeader(HttpHeaders.CONTENT_TYPE, "application/json")
            eb.send("books.post", json) { msg: AsyncResult<Message<JsonObject>> ->
                if (msg.succeeded()) {
                    response.setStatusCode(201).end(msg.result().body().encodePrettily())
                } else {
                    response.setStatusCode(HttpResponseStatus.BAD_REQUEST.code()).end(msg.cause().message)
                }
            }
        }
        //update
        router.route(HttpMethod.PUT, "/books/:id").handler(){ rt ->
            val id = Integer.parseInt(rt.pathParam("id"))
            val json = rt.bodyAsJson
            val response = rt.response()
            json.put("_id", id)
            response.putHeader(HttpHeaders.CONTENT_TYPE, "application/json")
            eb.send("books.put", json) { msg: AsyncResult<Message<JsonObject>> ->
                if (msg.succeeded()) {
                    response.setStatusCode(201).end(msg.result().body().encodePrettily())
                } else {
                    response.setStatusCode(HttpResponseStatus.BAD_REQUEST.code()).end(msg.cause().message)
                }
            }
        }
        //list
        router.route(HttpMethod.GET, "/books").handler(){ rt ->
            val response = rt.response()
            response.putHeader(HttpHeaders.CONTENT_TYPE, "application/json")
            eb.send("books.get","" ) { msg: AsyncResult<Message<JsonArray>> ->
                if (msg.succeeded()) {
                    response.setStatusCode(201).end(msg.result().body().encodePrettily())
                } else {
                    response.setStatusCode(HttpResponseStatus.BAD_REQUEST.code()).end(msg.cause().message)
                }
            }
        }
        //delete
        router.route(HttpMethod.DELETE, "/books/:id").handler(){ rt ->
            val id = Integer.parseInt(rt.pathParam("id"))

            val response = rt.response()

            response.putHeader(HttpHeaders.CONTENT_TYPE, "application/json")
            eb.send("books.delete", id) { msg: AsyncResult<Message<String>> ->
                if (msg.succeeded()) {
                    val res =  JsonObject().put("success",msg.result().body())
                    response.setStatusCode(201).end(res.encodePrettily())
                } else {
                    response.setStatusCode(HttpResponseStatus.BAD_REQUEST.code()).end(msg.cause().message)
                }
            }
        }
        return router
    }

    fun deployVerticles(): Future<Void> {
        return getDeploymentOptions()
            .compose { deploymentOptions ->
                config().mergeIn(deploymentOptions.config)
                return@compose Future.succeededFuture(deploymentOptions)
            }.compose { deploymentOptions ->
                val deployed = listOf(
                    deployVerticle(BookRepositoryVerticle::class.qualifiedName, 1, deploymentOptions),
                    deployVerticle(MQTTVerticle::class.qualifiedName, 1, deploymentOptions)
                )
                return@compose CompositeFuture.all(deployed).compose { _ -> Future.succeededFuture<Void>() }
            }

    }

    fun getDeploymentOptions(): Future<DeploymentOptions> {
        val future = Future.future<DeploymentOptions>()
        var retriever = ConfigRetriever.create(vertx)
        retriever.getConfig() { handler ->
            if (handler.succeeded()) {
                val config = JsonObject()
                config.mergeIn(handler.result())
                future.complete(DeploymentOptions().setConfig(config))
            } else {
                future.fail(handler.cause())
            }
        }
        return future
    }

    fun deployVerticle(name: String?, countInstances: Int, options: DeploymentOptions): Future<Void> {
        val future = Future.future<Void>()
        if (countInstances > 1) {
            options.instances = countInstances;
        }
        vertx.deployVerticle(name, options) { result ->
            if (result.succeeded()) {
                future.complete()
            } else {
                future.fail(result.cause())
            }
        }
        return future;

    }

}
