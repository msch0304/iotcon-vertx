package de.tarent.academy.iotcon.vertx.mqtt;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.mongo.MongoClient;

public class BookRepositoryVerticle extends AbstractVerticle {

    MongoClient client;

    final static String COLLECTION = "books";
    final static String COUNTER = "counter";
    final static String SEQUENCE = "bookid";
    final static String SEQUENCE_VALUE = "sequence_value";

    final static String[] NECESSARY = {"author", "title"};

    @Override
    public void start(Future<Void> startFuture) throws Exception {

        EventBus eb = vertx.eventBus();
        eb.consumer("books.post", msg -> createBook(msg));


    }

    private MongoClient getMongoClient(){
        if (client == null){
            client = MongoClient.createShared(vertx, new JsonObject().put("connection_string", config().getValue("mongo_url")), "iotcon");
        }
        return client;
    }

    private boolean checkMandantory(JsonObject o){
        boolean isComplete = true;
        for (String key:NECESSARY){
            isComplete = o.containsKey(key);
        }
        return isComplete;
    }


    private Future<Integer> findNextId(){
        Future future = Future.future();
        MongoClient mongoclient = getMongoClient();

        JsonObject query = new JsonObject().put("_id", SEQUENCE);
        JsonObject update = new JsonObject().put("$inc", new JsonObject().put(SEQUENCE_VALUE, 1));
        mongoclient.findOneAndUpdate(COUNTER, query, update, handler -> {
            if (handler.succeeded()){
                JsonObject sequence = handler.result();
                future.complete(sequence.getInteger(SEQUENCE_VALUE));
            }else{
                future.fail("New ID couldn't be generated");
            }
        });
        return future;
    }

    private void createBook(Message<Object> msg) {
        JsonObject book = (JsonObject) msg.body();

        if (checkMandantory(book)) {
            Future<Integer> future = findNextId();
            future.setHandler(handler -> {
                if (handler.succeeded()){
                    book.put("_id", handler.result());
                    MongoClient mongoClient = getMongoClient();
                    mongoClient.insert(COLLECTION, book, resHandler -> {
                        if (resHandler.succeeded()){
                            JsonObject toResponse = new JsonObject();
                            toResponse.put("success", resHandler.result());
                            msg.reply(toResponse);
                        }else{
                            msg.fail(1000, resHandler.cause().getMessage());
                        }

                    });
                }else {
                    msg.fail(1000, handler.cause().getMessage());
                }
            });
        } else{
            msg.fail(1000, "Bokk doesn't contains all mandantory fields");
        }
    }
}
