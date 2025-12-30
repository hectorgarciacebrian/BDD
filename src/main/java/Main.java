import java.sql.Connection;
public class Main {
    public static void main(String[] args) {
        System.out.println("--- TEST DE CONEXION UGR ---");
        
        DatabaseManager db = DatabaseManager.getInstance();

        for (DatabaseManager.Delegacion d : DatabaseManager.Delegacion.values()) {
            System.out.print("Conectando a " + d + "... ");
            try (Connection conn = db.getConnection(d)) {
                System.out.println("EXITO (Usuario: " + conn.getMetaData().getUserName() + ")");
            } catch (Exception e) {
                System.out.println("FALLO: " + e.getMessage());
            }
        }
    }
}