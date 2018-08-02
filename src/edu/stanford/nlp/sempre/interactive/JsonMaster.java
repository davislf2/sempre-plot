package edu.stanford.nlp.sempre.interactive;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

import edu.stanford.nlp.sempre.*;
import fig.basic.LogInfo;
import fig.basic.Option;

/**
 * Handle queries in Json, since embedding json into LispTree is bad
 */
public class JsonMaster extends Master {
  public static class Options {
    @Option(gloss = "Write out new grammar rules")
    public String intOutputPath;
    @Option(gloss = "each session gets a different model with its own parameters")
    public boolean independentSessions = false;
    @Option(gloss = "number of utterances to return for autocomplete")
    public int autocompleteCount = 5;
    @Option(gloss = "only allow interactive commands")
    public boolean onlyInteractive = false;

    @Option(gloss = "allow regular commands specified in Master")
    public boolean allowRegularCommands = false;

    @Option(gloss = "initial training set")
    public String dataPath = "";
  }

  public static Options opts = new Options();

  public JsonMaster(Builder builder) {
    super(builder);
  }

  @Override
  protected void printHelp() {
    // interactive commands
    LogInfo.log("Should not be run from commandline");
    super.printHelp();
  }

  @Override
  public void runServer() {
    InteractiveServer server = new InteractiveServer(this);
    server.run();
  }

  private String fakeQuery(String line) {
    return String.format("[\"q\", {\"utterance\": \"%s\", \"context\":{}, \"schema\":", line)
      + "{\"a\":{\"name\":\"a\",\"type\":\"string\",\"uniqueCount\":3,\"count\":9,\"probablyYears\":false,\"source\":true},"
      + "\"b\":{\"name\":\"b\",\"type\":\"integer\",\"uniqueCount\":6,\"count\":9,\"probablyYears\":false,\"source\":true}}}]";
  }

  @Override
  public Response processQuery(Session session, String line) {
    if (session.id.equals("stdin")) {
      if (line.startsWith("(")) {
        return super.processQuery(session, line);
      } else {
        line = fakeQuery(line);
      }
    }
    LogInfo.begin_track("JsonMaster.handleQuery");
    LogInfo.logs("session %s", session.id);
    LogInfo.logs("query %s", line);
    line = line.trim();
    Response response = new Response();
    handleCommand(session, line, response);
    LogInfo.end_track();
    return response;
  }

  @SuppressWarnings("unchecked")
  void handleCommand(Session session, String line, Response response) {
    List<Object> args = Json.readValueHard(line, List.class);
    String command = (String) args.get(0);
    Map<String, Object> kv = (Map<String, Object>) args.get(1);
    QueryStats stats = new QueryStats(response, command);

    // Start of interactive commands
    if (command.equals("q")) {
      /* Issue a query. This will create a new Example.
       *
       * Usage:
       *   ["q", {
       *     "utterance": utterance (string),
       *     "context": Vega-lite context (object),
       *     "schema": schema map (object),
       *     "random": randomize the order (true | false),
       *     "amount": number of candidates to request (integer)
       *   }]
       *
       * - If context is an empty object
       *     Parse the command for generating a new plot
       * - Otherwise:
       *     Parse the command, either for modifying the plot or generating a new plot
       */
      String utt = (String) kv.get("utterance");
      session.context = VegaJsonContextValue.fromClientRequest(kv);

      // Create the example
      Example ex = exampleFromUtterance(utt, session);
      builder.parser.parse(builder.params, ex, false);
      stats.size(ex.predDerivations != null ? ex.predDerivations.size() : 0);
      stats.status(InteractiveUtils.getParseStatus(ex));
      session.updateContext();
      LogInfo.logs("parse stats: %s", response.stats);

      int amount = ex.predDerivations.size();
      int size = amount;
      if (kv.containsKey("amount")) {
        amount = (int) kv.get("amount");
      }
      List<Derivation> top = ex.predDerivations.subList(0, 0);
      List<Derivation> rest = ex.predDerivations.subList(0, size);
      if (kv.containsKey("random") && (boolean)kv.get("random")) {
        Collections.shuffle(rest);
      }
      top.addAll(rest);
      ex.predDerivations = top.subList(0, amount > size ? size : amount);
      response.ex = ex;
    } else if (command.equals("random")) {
      /* Generate random derivations
       *
       * Usage:
       *   ["random", {
       *     "amount": amount (int),
       *     "context": Vega-lite context (object),
       *     "schema": schema map (object),
       *   }]
       *
       * - If context is an empty object or contains "initialContext" key:
       *     Suggest possible plots based on the table fields
       * - Otherwise:
       *     Suggest possible modifications to the current plot
       */
      int amount = (int) kv.get("amount");
      VegaJsonContextValue context = VegaJsonContextValue.fromClientRequest(kv);
      session.context = context;
      Example ex = exampleFromUtterance("", session);
      VegaRandomizer randomizer = new VegaRandomizer(ex, builder);
      if (context.isInitialContext())
        response.ex = randomizer.generateInitial(amount);
      else
        response.ex = randomizer.generateModification(amount);

    } else if (command.equals("accept")) {
      /* Accept the user's selection.
       *
       * Usage:
       *   ["accept", {
       *     "type": type (string),
       *     "utterance": utterance (string),
       *     "context": Vega-lite context (object),
       *     "schema": schema map (object),
       *     "targetValue": targetValue (...),
       *     "targetFormula": targetFormula (...),
       *     "issuedQuery": issuedQuery (string; only for type = label),
       *   }]
       *
       * Using lastExample seems unreliable, different tabs etc.
       */
      String utt = (String) kv.get("utterance");
      Object targetValue = kv.get("targetValue");
      Example ex = exampleFromUtterance(utt, session);
      ex.targetValue = new JsonValue(targetValue);
      ex.context = VegaJsonContextValue.fromClientRequest(kv);
      builder.parser.parse(builder.params, ex, true);

      if (Master.opts.onlineLearnExamples)
        learner.onlineLearnExample(ex);

    } else if (command.equals("reject")) {
      /* Reject a plot.
       *
       * Usage:
       *   ["reject", {
       *     "utterance": utterance (string),
       *     "context": Vega-lite context (object),
       *     "schema": schema map (object),
       *     "targetValue": targetValue (...),
       *   }]
       */
      // TODO
    } else if (command.equals("example")) {
      /* Fetch an example specification
       *
       * Usage:
       *   ["example" {
       *     "schema": schema map (object),
       *     "amount": the number of examples to be fetched, return all when unspecified
       *   }]
       */
      List<Map<String, Object>> examples = VegaResources.getExamples();
      List<Map<String, Object>> randomExamples = new ArrayList<>(examples);
      Collections.shuffle(randomExamples);
      if (kv.containsKey("amount")) {
        int amount = (int) kv.get("amount");
        int size = randomExamples.size();
        randomExamples = randomExamples.subList(0, amount > size ? size : amount);
      }
      response.lines = randomExamples.stream().map(ex -> Json.writeValueAsStringHard(ex)).collect(Collectors.toList());
    } else if (command.equals("log")) {
      /* Used to log information that is not acted upon
       * Usage: ["log", object]
       */
    } else {
      LogInfo.log("Invalid command: " + args);
    }
  }

  private static Example exampleFromUtterance(String utt, Session session) {
    Example.Builder b = new Example.Builder();
    b.setId(session.id);
    b.setUtterance(utt);
    b.setContext(session.context);
    Example ex = b.createExample();
    ex.preprocess();
    return ex;
  }
}
