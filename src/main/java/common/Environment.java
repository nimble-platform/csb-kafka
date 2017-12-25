package common;

/**
 * Created by evgeniyh on 06/04/17.
 */
public enum Environment {
    DEVELOPMENT {
        public String toString() {
            return "dev";
        }
    },
    PRODUCTION {
        public String toString() {
            return "prod";
        }
    }
}
