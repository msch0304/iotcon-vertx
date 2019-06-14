package de.tarent.academy.iotcon.vertx.mqtt

import io.netty.handler.codec.http.HttpResponseStatus
import io.vertx.config.ConfigRetriever
import io.vertx.core.AbstractVerticle
import io.vertx.core.AsyncResult
import io.vertx.core.DeploymentOptions
import io.vertx.core.Future
import io.vertx.core.eventbus.Message
import io.vertx.core.http.HttpHeaders
import io.vertx.core.http.HttpMethod
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
      .requestHandler (router)
      .listen(8888) { http ->
        if (http.succeeded()) {
          startFuture.complete()
          println("HTTP server started on port 8888")
        } else {
          startFuture.fail(http.cause());
        }
      }
  }

  fun defineRouter():Router {
    var router = Router.router(vertx)
    router.route().handler(BodyHandler.create());
    val eb = vertx.eventBus()

    var create = router.route(HttpMethod.POST, "/books")
    create.handler(){ rt ->
        println (rt.bodyAsString)
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
    return router
  }

  fun deployVerticles (): Future<Void> {
      var future = Future.future<Void>()
      var retriever = ConfigRetriever.create(vertx)
      retriever.getConfig() { handler ->
          if (handler.succeeded()) {
              val config = handler.result()
              val deploymentOptions = DeploymentOptions()
              deploymentOptions.config = config
              vertx.deployVerticle(
                  de.tarent.academy.iotcon.vertx.mqtt.BookRepositoryVerticle::class.qualifiedName,
                  deploymentOptions
              ){
                  if (it.succeeded()){
                      future.complete()
                  }else{
                      future.fail("Deployment of Repo failed")
                  }
              }
          }
      }
      return future;
  }
}
