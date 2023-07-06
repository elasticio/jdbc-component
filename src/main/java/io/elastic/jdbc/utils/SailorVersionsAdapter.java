package io.elastic.jdbc.utils;

import com.google.gson.JsonParser;

import jakarta.json.Json;
import jakarta.json.JsonReader;
import java.io.StringReader;

public class SailorVersionsAdapter {

    public static jakarta.json.JsonObject gsonToJavax(com.google.gson.JsonObject json) {
        JsonReader jsonReader = Json.createReader(new StringReader(json.toString()));
        jakarta.json.JsonObject jsonObject = jsonReader.readObject();
        jsonReader.close();

        return jsonObject;
    }

    public static com.google.gson.JsonObject javaxToGson(jakarta.json.JsonObject json) {
        return new JsonParser().parse(json.toString()).getAsJsonObject();
    }

}