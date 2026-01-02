import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class CreacionVistas {

    public static void main(String[] args) {
        DatabaseManager db = DatabaseManager.getInstance();
        System.out.println("--- CREANDO VISTAS DEL SISTEMA ---");

        // 1. Vista de la agregación sucursal y vino ofertado
        String viewOferta = """
            CREATE OR REPLACE VIEW Vista_Oferta_Sucursal AS
            SELECT 
                s.nombre AS Sucursal, 
                s.ciudad,
                v.vinedo AS Vino, 
                v.stock, 
                v.c_producida
            FROM Suministra sum
            JOIN Sucursal s ON sum.cod_sucursal = s.cod_sucursal
            JOIN Vino v     ON sum.cod_vino = v.cod_vino
        """;

        // 2. Vista de pedidos de clientes
        String viewPedidosClientes = """
            CREATE OR REPLACE VIEW Vista_Pedidos_Clientes AS
            SELECT 
                p.fecha_pide, 
                c.nombre_c AS Cliente, 
                v.vinedo AS Vino_Solicitado, 
                s.nombre AS Sucursal, 
                p.cantidad
            FROM Pide p
            JOIN Cliente c  ON p.cod_cliente = c.cod_c
            JOIN Vino v     ON p.cod_vino = v.cod_vino
            JOIN Sucursal s ON p.cod_sucursal = s.cod_sucursal
        """;

        // 3. Vista entre pedidos de sucursales
        String viewPedidosSucursales = """
            CREATE OR REPLACE VIEW Vista_Logistica_Sucursales AS
            SELECT 
                sol.fecha_sol, 
                s_origen.nombre AS Sucursal_Pide, 
                s_destino.nombre AS Sucursal_Provee, 
                v.vinedo AS Vino, 
                sol.cantidad
            FROM Solicita sol
            JOIN Sucursal s_origen  ON sol.cod_sucursal = s_origen.cod_sucursal
            JOIN Sucursal s_destino ON sol.cod_sucursal_prov = s_destino.cod_sucursal
            JOIN Vino v             ON sol.cod_tipo_vino = v.cod_vino
        """;

        String[] vistas = { viewOferta, viewPedidosClientes, viewPedidosSucursales };

        for (DatabaseManager.Delegacion d : DatabaseManager.Delegacion.values()) {
            System.out.println("\nNodo: " + d);
            try (Connection conn = db.getConnection(d);
                 Statement stmt = conn.createStatement()) {

                System.out.print("Generando vistas... ");
                for (String sql : vistas) {
                    try {
                        stmt.executeUpdate(sql);
                    } catch (SQLException e) {
                        System.err.println("\nError vista: " + e.getMessage());
                    }
                }
                System.out.println("OK.");

            } catch (SQLException e) {
                System.err.println("Error conexión: " + e.getMessage());
            }
        }
        System.out.println("\nPROCESO FINALIZADO.");
    }
}