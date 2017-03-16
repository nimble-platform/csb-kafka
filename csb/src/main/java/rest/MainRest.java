package rest;

/**
 * Created by evgeniyh on 16/03/17.
 */
public class MainRest {
    public static void main(String[] args) {
        try {
            RestCSB csb = new RestCSB();
            csb.start();
        } catch (Exception ex) {
            System.out.print(ex.getMessage());
            ex.printStackTrace();
        }
    }
}
