package hello;

import org.slf4j.Logger;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.bind.WebDataBinder;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.InitBinder;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

@SpringBootApplication
@RestController
public class Application {

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
        return "Let the battle begin!";
    }

    @PostMapping("/**")
    public String index(@RequestBody ArenaUpdate arenaUpdate) {
        System.out.println(arenaUpdate);
        String command = getCommand(arenaUpdate);
        log.info("executing {}", command);
        return command;
    }

    private String getCommand(ArenaUpdate arenaUpdate) {
        String[] commands = new String[]{"F", "R", "L"};
        int i = new Random().nextInt(3);

        String self = arenaUpdate._links.self.href;
        PlayerState me = arenaUpdate.arena.state.get(self);
        List<PlayerState> others = getOthers(arenaUpdate, self);

        if (me.wasHit) {
            log.info("I was hit");
            return "F";
        }

        if (isAnyoneInFrontOfMe(me, others)) {
            return "T";
        } else {

            if (me.wasHit) {
                log.info("Fleeing");
                return "F";
            }
            counter++;
            if (counter > 2) {
                counter = 0;
                return "F";
            }
            return "L";
        }
    }

    private List<PlayerState> getOthers(ArenaUpdate arenaUpdate, String self) {
        return arenaUpdate.arena.state.entrySet().stream()
                .filter(e -> !e.getKey().equals(self))
                .map(Map.Entry::getValue)
                .collect(Collectors.toList());
    }

    private boolean isAnyoneInFrontOfMe(PlayerState me, List<PlayerState> others) {
        Set<Integer> oy = others.stream()
                .filter(e -> e.x.equals(me.x))
                .map(e -> e.y)
                .collect(Collectors.toSet());
        Set<Integer> ox = others.stream()
                .filter(e -> e.y.equals(me.y))
                .map(e -> e.x)
                .collect(Collectors.toSet());

        log.info("Facing {}. Players on same X {}: %s. On Y: {} ",  me.direction, ox.size(), oy.size());

        boolean facingSomeone;

        switch (me.direction) {
            case "N":
                facingSomeone = oy.stream().anyMatch(y -> me.y - y <= 3);
                break;
            case "W":
                facingSomeone = ox.stream().anyMatch(x -> me.x - x <= 3);
                break;
            case "S":
                facingSomeone = oy.stream().anyMatch(y -> y - me.y <= 3);
                break;
            case "E":
                facingSomeone = ox.stream().anyMatch(x -> x - me.x <= 3);
                break;
            default:
                facingSomeone = false;
        }
        if (facingSomeone) {
            log.info("Found someone");
        }
        return facingSomeone;
    }

}
