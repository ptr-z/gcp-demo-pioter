package hello;

import com.google.api.core.ApiFuture;
import com.google.cloud.ServiceOptions;
import com.google.cloud.bigquery.storage.v1.*;
import com.google.protobuf.Descriptors;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;
import java.time.Instant;


import org.slf4j.Logger;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.bind.WebDataBinder;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.InitBinder;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@SpringBootApplication
@RestController
public class Application {

    static boolean moveNotTurn = true;
    static String currentTurn = "L";

    private static final Logger log = org.slf4j.LoggerFactory.getLogger(Application.class);
    static int counter;

    static class Self {
        public String href;
    }

    static class Links {
        public Self self;
    }

    static class PlayerState {
        public Integer x;
        public Integer y;
        public String direction;
        public Boolean wasHit;
        public Integer score;

        @Override
        public String toString() {
            return "State: {" +
                    "x=" + x +
                    ", y=" + y +
                    ", direction='" + direction + '\'' +
                    ", wasHit=" + wasHit +
                    ", score=" + score +
                    '}';
        }
    }

    static class Arena {
        public List<Integer> dims;
        public Map<String, PlayerState> state;
    }

    static class ArenaUpdate {
        public Links _links;
        public Arena arena;
    }

    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }

    @InitBinder
    public void initBinder(WebDataBinder binder) {
        binder.initDirectFieldAccess();
    }

    @GetMapping("/")
    public String index() {
        return "Lorem ipsum5";
    }

    @PostMapping("/**")
    public String index(@RequestBody ArenaUpdate arenaUpdate) {
        writeCommittedStream.send(arenaUpdate.arena);
        System.out.println(arenaUpdate);
        String command = getCommand(arenaUpdate);
        log.info("executing {}", command);
        return command;
    }

    private String getCommand(ArenaUpdate arenaUpdate) {
        String self = arenaUpdate._links.self.href;
        PlayerState me = arenaUpdate.arena.state.get(self);
        List<PlayerState> others = getOthers(arenaUpdate, self);

        Integer xBorder = others.stream().map(p -> p.x).max(Integer::compare).orElseThrow();
        Integer yBorder = others.stream().map(p -> p.y).max(Integer::compare).orElseThrow();

        log.info(String.valueOf(me));


        if (isAnyoneClose(me, others)) {
            return "T";
        } else {
            if (me.wasHit) {
                log.info("I was hit - fleeing");
                return "F";
            }

            if(moveNotTurn){
                moveNotTurn = false;
            } else {
                moveNotTurn = true;
                if(Set.of(0, xBorder).contains(me.x) || Set.of(0, yBorder).contains(me.y)){
                    currentTurn = currentTurn.equals("L") ? "R" : "L";
                }
                return currentTurn;
            }
        }
    }

    private List<PlayerState> getOthers(ArenaUpdate arenaUpdate, String self) {
        return arenaUpdate.arena.state.entrySet().stream()
                .filter(e -> !e.getKey().equals(self))
                .map(Map.Entry::getValue)
                .collect(Collectors.toList());
    }

    private boolean isAnyoneClose(PlayerState me, List<PlayerState> others) {
        Set<Integer> oy = others.stream()
                .filter(e -> e.x.equals(me.x))
                .map(e -> e.y)
                .collect(Collectors.toSet());
        Set<Integer> ox = others.stream()
                .filter(e -> e.y.equals(me.y))
                .map(e -> e.x)
                .collect(Collectors.toSet());

        log.info("Facing {}. Players on same X {}: %s. On Y: {} ", me.direction, ox.size(), oy.size());
        Set<Integer> found;

        switch (me.direction) {
            case "N":
                found = oy.stream().filter(y -> y < me.y).filter(y -> me.y - y <= 3).collect(Collectors.toSet());
                break;
            case "W":
                found = ox.stream().filter(x -> x < me.x).filter(x -> me.x - x <= 3).collect(Collectors.toSet());
                break;
            case "S":
                found = oy.stream().filter(y -> y > me.y).filter(y -> y - me.y <= 3).collect(Collectors.toSet());
                break;
            case "E":
                found = ox.stream().filter(x -> x > me.x).filter(x -> x - me.x <= 3).collect(Collectors.toSet());
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + me.direction);
        }

        boolean facingSomeone = found.size() > 0;

        if (facingSomeone) {
            log.info("Found someone (x:{} y:{}) {}: {}", me.x, me.y, me.direction, Arrays.stream(found.toArray()).toArray());
        }
        return facingSomeone;
    }


    static class WriteCommittedStream {

        final JsonStreamWriter jsonStreamWriter;

        public WriteCommittedStream(String projectId, String datasetName, String tableName) throws IOException, Descriptors.DescriptorValidationException, InterruptedException {

            try (BigQueryWriteClient client = BigQueryWriteClient.create()) {

                WriteStream stream = WriteStream.newBuilder().setType(WriteStream.Type.COMMITTED).build();
                TableName parentTable = TableName.of(projectId, datasetName, tableName);
                CreateWriteStreamRequest createWriteStreamRequest =
                        CreateWriteStreamRequest.newBuilder()
                                .setParent(parentTable.toString())
                                .setWriteStream(stream)
                                .build();

                WriteStream writeStream = client.createWriteStream(createWriteStreamRequest);

                jsonStreamWriter = JsonStreamWriter.newBuilder(writeStream.getName(), writeStream.getTableSchema()).build();
            }
        }

        public ApiFuture<AppendRowsResponse> send(Arena arena) {
            Instant now = Instant.now();
            JSONArray jsonArray = new JSONArray();

            arena.state.forEach((url, playerState) -> {
                JSONObject jsonObject = new JSONObject();
                jsonObject.put("x", playerState.x);
                jsonObject.put("y", playerState.y);
                jsonObject.put("direction", playerState.direction);
                jsonObject.put("wasHit", playerState.wasHit);
                jsonObject.put("score", playerState.score);
                jsonObject.put("player", url);
                jsonObject.put("timestamp", now.getEpochSecond() * 1000 * 1000);
                jsonArray.put(jsonObject);
            });

            return jsonStreamWriter.append(jsonArray);
        }

    }

    final String projectId = ServiceOptions.getDefaultProjectId();
    final String datasetName = "snowball";
    final String tableName = "events";

    final WriteCommittedStream writeCommittedStream;

    public Application() throws Descriptors.DescriptorValidationException, IOException, InterruptedException {
        writeCommittedStream = new WriteCommittedStream(projectId, datasetName, tableName);
    }
}
