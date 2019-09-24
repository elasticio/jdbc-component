package io.elastic.jdbc.utils;

import com.google.gson.JsonParser;

import javax.json.Json;
import javax.json.JsonReader;
import java.io.StringReader;

public class SailorVersionsAdapter {

    public static javax.json.JsonObject gsonToJavax(com.google.gson.JsonObject json) {
        JsonReader jsonReader = Json.createReader(new StringReader(json.toString()));
        javax.json.JsonObject jsonObject = jsonReader.readObject();
        jsonReader.close();

        return jsonObject;
    }

    public static com.google.gson.JsonObject javaxToGson(javax.json.JsonObject json) {
        return new JsonParser().parse(json.toString()).getAsJsonObject();
    }

}