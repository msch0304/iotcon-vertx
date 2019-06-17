package de.tarent.academy.iotcon.vertx.mqtt;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.mongo.MongoClient;

public class BookRepositoryVerticle extends AbstractVerticle {

    MongoClient client;

    final static String COLLECTION = "books";
    final static String COUNTER = "counters";
    final static String SEQUENCE = "bookid";
    final static String SEQUENCE_VALUE = "sequence_value";

    final static String[] NECESSARY = {"author", "title"};

    @Override
    public void start(Future<Void> startFuture) throws Exception {

        EventBus eb = vertx.eventBus();
        eb.consumer("books.post", msg -> createBook(msg));
        eb.consumer("books.put", msg -> updateBook(msg));
        eb.consumer("books.get", msg -> listBooks(msg));
        eb.consumer("books.delete", msg -> deleteBook(msg));
        startFuture.complete();

    }

    private MongoClient getMongoClient(){
        if (client == null){
            client = MongoClient.createShared(vertx, new JsonObject().put("connection_string", config().getValue("mongo_url")), "iotcon");
        }
        return client;
    }

    /**
     * book needs author and title
     * @param book - JsonObject with
     * @return
     */
    private boolean checkMandantory(JsonObject book){
        boolean isComplete = true;
        for (String key:NECESSARY){
            isComplete = book.containsKey(key);
        }
        return isComplete;
    }

    /**
     * sequence is implemented through a counters collection
     * @return JsonObject with increment
     */
    private JsonObject createSequenceCommand() {
        JsonObject updateCommand = new JsonObject().put("$inc", new JsonObject().put(SEQUENCE_VALUE, 1L));
        return updateCommand;
    }

    /**
     * finAndModify has to be implemented through a command so these methods creates the object with the command parameters
     * @param query
     * @param updateCommand
     * @return
     */
    private JsonObject createFindAndModify(JsonObject query, JsonObject updateCommand) {
        JsonObject retOb = new JsonObject();
        retOb.put("findAndModify", COUNTER);
        retOb.put("query", query);
        retOb.put("update", updateCommand);
        retOb.put("new", true);
        return retOb;
    }

    /**
     * ircremented counter for new books as key
     * @return
     */
    private Future<Integer> findNextId(){
        Future future = Future.future();
        MongoClient mongoclient = getMongoClient();

        JsonObject query = new JsonObject().put("_id", SEQUENCE);
        JsonObject update = createSequenceCommand();
        JsonObject command = createFindAndModify(query, update);
        mongoclient.runCommand("findAndModify", command, handler -> {
            if (handler.succeeded()){
                System.out.println (handler.result());
                JsonObject sequence = handler.result().getJsonObject("value");

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
                            msg.reply(book);
                        }else{
                            msg.fail(1000, resHandler.cause().getMessage());
                        }

                    });
                }else {
                    msg.fail(1000, handler.cause().getMessage());
                }
            });
        } else{
            msg.fail(1000, "Book doesn't contains all mandantory fields");
        }
    }

    private void updateBook(Message<Object> msg) {
        JsonObject book = (JsonObject) msg.body();

        if (checkMandantory(book) && book.containsKey("_id")) {

            MongoClient mongoClient = getMongoClient();
            JsonObject update = new JsonObject().put("$set", book);
            JsonObject query = new JsonObject().put("_id", book.getInteger("_id"));
            mongoClient.findOneAndUpdate(COLLECTION, query, update, resHandler -> {
                if (resHandler.succeeded()){
                    JsonObject toResponse = new JsonObject();
                    toResponse.put("success", resHandler.result());
                    msg.reply(book);
                }else{
                    msg.fail(1000, resHandler.cause().getMessage());
                }

            });
        } else{
            msg.fail(1000, "Book doesn't contains all mandantory fields");
        }
    }

    private void listBooks(Message<Object> msg) {

        MongoClient mongoClient = getMongoClient();
        mongoClient.find(COLLECTION, new JsonObject(), resHandler -> {
            if (resHandler.succeeded()){
                JsonArray arr = new JsonArray(resHandler.result());
                msg.reply(arr);
            }else{
                msg.fail(1000, resHandler.cause().getMessage());
            }
        });
    }

    private void deleteBook(Message<Object> msg) {
        int id  = (Integer) msg.body();
        MongoClient mongoClient = getMongoClient();

        JsonObject query = new JsonObject().put("_id", id);
        mongoClient.findOneAndDelete(COLLECTION, query, resHandler -> {
            if (resHandler.succeeded()){
                JsonObject toResponse = new JsonObject();
                toResponse.put("success", resHandler.result());
                msg.reply(id + "deleted");
            }else{
                msg.fail(1000, resHandler.cause().getMessage());
            }

        });


    }

}
