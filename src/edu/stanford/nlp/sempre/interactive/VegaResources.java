package edu.stanford.nlp.sempre.interactive;


import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

import com.google.common.collect.Sets;
import org.testng.util.Strings;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.Lists;

import edu.stanford.nlp.sempre.Json;
import edu.stanford.nlp.sempre.JsonValue;
import fig.basic.IOUtils;
import fig.basic.LogInfo;
import fig.basic.MapUtils;
import fig.basic.Option;

import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Stream;


/**
 * Vega-specific code that loads the schema, colors, and generate paths and type maps
 * @author sidaw
 */

@JsonIgnoreProperties(ignoreUnknown = true)
public class VegaResources {
  public static class Options {
    @Option(gloss = "File containing the vega schema") String vegaSchema;
    @Option(gloss = "Path elements to exclude") Set<String> excludedPaths;
    @Option(gloss = "Paths in the context that are not considered for removals") Set<String> excludedContextPaths;
    @Option(gloss = "File containing all the colors") String colorFile;
    @Option(gloss = "File containing initial plot templates") String initialTemplates;
    @Option(gloss = "Path to a log of queries") String queryPath;
    @Option(gloss = "verbosity") int verbose = 0;
  }
  public static Options opts = new Options();
  private final Path savePath = Paths.get(JsonMaster.opts.intOutputPath, "vegaResource");

  public static VegaLitePathMatcher allPathsMatcher;
  private static List<List<String>> filteredPaths;
  private static List<JsonSchema> descendants;

  public static JsonSchema vegaSchema;

  private static Map<String, Set<String>> enumValueToTypes;
  private static Map<String, Set<List<String>>> enumValueToPaths;

  private static Set<String> colorSet;
  private static List<Map<String, Object>> examples;

  public static final Set<String> CHANNELS = Sets.newHashSet("x", "y", "color", "opacity", "shape", "size", "row", "column");
  public static final Set<String> MARKS = Sets.newHashSet("area", "bar", "circle", "line", "point", "rect", "rule", "square", "text", "tick");
  public static final Set<String> AGGREGATES = Sets.newHashSet("max", "mean", "min", "median", "sum");
  static class InitialTemplate {
    @JsonProperty("mark") public String mark;
    @JsonProperty("encoding") public Map<String, String> encoding;
  }
  private static List<InitialTemplate> initialTemplates;

  public VegaResources() {
    try {
      if (!Strings.isNullOrEmpty(opts.vegaSchema)) {
        LogInfo.begin_track("Loading schemas from %s", opts.vegaSchema);
        vegaSchema = JsonSchema.fromFile(new File(opts.vegaSchema));
        LogInfo.end_track();
      }

      List<JsonSchema> allDescendants = vegaSchema.descendants();
      descendants = allDescendants.stream().filter(s -> s.node().has("type")).collect(Collectors.toList());
      LogInfo.logs("Got %d descendants, %d typed", allDescendants.size(), descendants.size());
      Json.prettyWriteValueHard(new File(savePath.toString()+".nodes.json"),
        descendants.stream().map(t -> t.node()).collect(Collectors.toList()));

      filteredPaths = allSimplePaths(descendants);
      LogInfo.logs("Got %d distinct simple path not containing %s", filteredPaths.size(), opts.excludedPaths);
      allPathsMatcher = new VegaLitePathMatcher(filteredPaths);
      Json.prettyWriteValueHard(new File(savePath.toString()+".simplePaths.json"), filteredPaths);

      // generate valueToTypes and valueToSet, for enum types
      generateValueMaps();
      LogInfo.logs("gathering valueToTypes: %d distinct enum values", enumValueToTypes.size());
      Json.prettyWriteValueHard(new File(savePath.toString()+".enums.json"),
        enumValueToTypes.keySet().stream().collect(Collectors.toList())
      );

      if (!Strings.isNullOrEmpty(opts.colorFile)) {
        colorSet = Json.readMapHard(String.join("\n", IOUtils.readLines(opts.colorFile))).keySet();
        LogInfo.logs("loaded %d colors from %s", colorSet.size(), opts.colorFile);
      }

      if (!Strings.isNullOrEmpty(opts.initialTemplates)) {
        initialTemplates = new ArrayList<>();
        for (JsonNode node : Json.readValueHard(String.join("\n", IOUtils.readLines(opts.initialTemplates)), JsonNode.class)) {
          initialTemplates.add(Json.getMapper().treeToValue(node, InitialTemplate.class));
        }
        LogInfo.logs("Read %d initial templates", initialTemplates.size());
      }

      if (!Strings.isNullOrEmpty(opts.queryPath)) {
        Stream<String> stream = Files.lines(Paths.get(opts.queryPath));
        examples = stream.map(q -> Json.readMapHard(q)).filter(q -> ((List<?>)q.get("q")).get(0).equals("accept"))
                .collect(Collectors.toList());
      }

    } catch (Exception ex) {
      ex.printStackTrace();
      throw new RuntimeException(ex);
    }
  }

  private List<List<String>> allSimplePaths(List<JsonSchema> descendents) {
    LinkedHashSet<List<String>> simplePaths = descendents.stream()
      .map(s -> s.simplePath()).collect(Collectors.toCollection(LinkedHashSet::new));
    LogInfo.logs("Got %d distinct simple paths", simplePaths.size());

    return simplePaths.stream().filter(p -> p.stream().allMatch(s -> !opts.excludedPaths.contains(s)))
        .collect(Collectors.toList());
  }

  private void generateValueMaps() {
    Set<JsonSchema> descendentsSet = descendants.stream().collect(Collectors.toSet());
    enumValueToTypes = new HashMap<>();
    enumValueToPaths = new HashMap<>();
    for (JsonSchema schema: descendentsSet) {
      if (schema.enums() != null) {
        for (String e : schema.enums()) {
          MapUtils.addToSet(enumValueToTypes, e, schema.types().get(0));
          MapUtils.addToSet(enumValueToPaths, e, schema.simplePath());
        }
      }
    }
  }

  private static boolean checkType(List<String> path, JsonValue value) {
    JsonSchema jsonSchema = VegaResources.vegaSchema;
    List<JsonSchema> pathSchemas = jsonSchema.schemas(path);
    String stringValue = value.getJsonNode().asText();

    for (JsonSchema schema : pathSchemas) {
      String valueType = value.getSchemaType();
      List<String> schemaTypes = schema.types();
      if (opts.verbose > 1)
        System.out.println(String.format("checkType: path: %s | simplePath: %s | types: %s | valueType: %s", path, schema.simplePath(), schemaTypes, valueType));
      for (String schemaType : schemaTypes) {

        List<String> simplePath = schema.simplePath();
        String last = simplePath.get(simplePath.size() - 1);

        if (schemaType.equals("string") && (last.endsWith("color") || last.endsWith("Color")
            || last.equals("fill")
            || last.equals("stroke") || last.equals("background"))) {
          return valueType.equals("color");
        }

        if (schemaType.equals("string") && last.equals("field")) {
          return valueType.equals("field");
        }

        if (schemaType.equals("string") && schema.isEnum()) {
          return schema.enums().contains(stringValue);
        }

        if (valueType.equals(schemaType)) {
          return true;
        }

        if (schemaType.equals(JsonSchema.NOTYPE))
          throw new RuntimeException("JsonFn: schema has no type: " + schema);
      }
    }
    return false;
  }

  public static List<JsonValue> getValues(List<String> path, JsonValue value) {
    if (value != null) {
      if (checkType(path, value)) {
        return Lists.newArrayList(value);
      } else {
        return Lists.newArrayList();
      }
    }
    List<JsonValue> values = new ArrayList<>();
    List<JsonSchema> schemas = vegaSchema.schemas(path);
    for (JsonSchema schema : schemas) {
      for (String type : schema.types()) {
        if (opts.verbose > 0)
          LogInfo.logs("getValues %s %s", type, path.toString());

        List<String> simplePath = schema.simplePath();
        String last = simplePath.get(simplePath.size() - 1);
        if (type.equals(JsonSchema.NOTYPE)) {
          continue;
        } else if (type.equals("string")) {
          if (last.endsWith("color") || last.endsWith("Color")
            || last.equals("fill")
            || last.equals("stroke") || last.equals("background")) {
            values.add(new JsonValue("red").withSchemaType("string"));
            values.add(new JsonValue("blue").withSchemaType("string"));
            values.add(new JsonValue("green").withSchemaType("string"));
          } else if (last.equals("field")) {
//            values.add(new JsonValue("fieldName").withSchemaType("string"));
          } else if(last.endsWith("font") || last.endsWith("Font")) {
            values.add(new JsonValue("times").withSchemaType("string"));
            values.add(new JsonValue("monaco").withSchemaType("string"));
            values.add(new JsonValue("cursive").withSchemaType("string"));
          } else if (schema.isEnum()) {
            values.addAll(schema.enums().stream().map(s -> new JsonValue(s).withSchemaType("enum"))
              .collect(Collectors.toList()));
          } else {
            values.add(new JsonValue("XXYYZZ").withSchemaType(type));
          }
        } else if (type.equals("boolean")) {
          values.add(new JsonValue(true).withSchemaType("boolean"));
          values.add(new JsonValue(false).withSchemaType("boolean"));
        } else if (type.equals("number")) {
          values.add(new JsonValue(ThreadLocalRandom.current().nextInt(0, 50)).withSchemaType("number"));
          values.add(new JsonValue(0.1 * ThreadLocalRandom.current().nextInt(1, 10)).withSchemaType("number"));
        }
      }
    }

    if (values.size() == 0) return values;
    return Lists.newArrayList(values.get(ThreadLocalRandom.current().nextInt(values.size())));
  }

  public static Set<String> getEnumTypes(String value) {
    if (enumValueToTypes.containsKey(value)) return enumValueToTypes.get(value);
    return null;
  }

  public static List<Map<String, Object>> getExamples() {
    return examples;
  }

  public static Set<String> getColorSet() {
    return colorSet;
  }

  public static List<InitialTemplate> getInitialTemplates() {
    return initialTemplates;
  }
}
