import java.sql.*;

public class OperacionesBD {

    private DatabaseManager db;
    private DatabaseManager.Delegacion delegacionActual;

    public OperacionesBD(DatabaseManager.Delegacion delegacion) {
        this.db = DatabaseManager.getInstance();
        this.delegacionActual = delegacion;
    }

    // --- MÉTODO AUXILIAR PARA LLAMAR A PL/SQL ---
    private void ejecutarProcedimiento(String sqlCall, Object... params) {
        try (Connection conn = db.getConnection(delegacionActual);
             CallableStatement cs = conn.prepareCall(sqlCall)) {

            for (int i = 0; i < params.length; i++) {
                if (params[i] == null) {
                    cs.setNull(i + 1, Types.VARCHAR);
                } else if (params[i] instanceof String) {
                    cs.setString(i + 1, (String) params[i]);
                } else if (params[i] instanceof Integer) {
                    cs.setInt(i + 1, (Integer) params[i]);
                } else if (params[i] instanceof Double) {
                    cs.setDouble(i + 1, (Double) params[i]);
                } else if (params[i] instanceof java.sql.Date) {
                    cs.setDate(i + 1, (java.sql.Date) params[i]);
                } else {
                    cs.setObject(i + 1, params[i]);
                }
            }
            
            cs.execute();

        } catch (SQLException e) {
            System.err.println("   -> FALLO BD: " + e.getMessage());
        }
    }

    // 1. Alta Empleado
    public void altaEmpleado(int cod, String dni, String nombre, Date fecha, double sal, String dir, int suc) {
        ejecutarProcedimiento("{call pr_Alta_Empleado(?,?,?,?,?,?,?)}", cod, dni, nombre, fecha, sal, dir, suc);
    }

    // 2. Baja Empleado
    public void bajaEmpleado(int cod) {
        ejecutarProcedimiento("{call pr_Baja_Empleado(?)}", cod);
    }

    // 3. Modificar Salario
    public void modificarSalario(int cod, double sal) {
        ejecutarProcedimiento("{call pr_Modificar_Salario(?,?)}", cod, sal);
    }

    // 4. Trasladar Empleado
    public void trasladarEmpleado(int cod, int suc, String dir) {
        ejecutarProcedimiento("{call pr_Trasladar_Empleado(?,?,?)}", cod, suc, dir);
    }

    // 5. Alta Sucursal
    public void altaSucursal(int cod, String nom, String ciu, String ca, Integer dir) {
        ejecutarProcedimiento("{call pr_Alta_Sucursal(?,?,?,?,?)}", cod, nom, ciu, ca, dir);
    }

    // 6. Cambiar Director
    public void cambiarDirector(int suc, int dir) {
        ejecutarProcedimiento("{call pr_Cambiar_Director(?,?)}", suc, dir);
    }

    // 7. Alta Cliente
    public void altaCliente(int cod, String dni, String nom, String dir, String tipo, String ca) {
        ejecutarProcedimiento("{call pr_Alta_Cliente(?,?,?,?,?,?)}", cod, dni, nom, dir, tipo, ca);
    }

    // 8. Gestionar Suministro
    public void gestionarSuministro(int cli, int suc, int vino, Date fecha, int cant) {
        ejecutarProcedimiento("{call pr_Gestionar_Suministro(?,?,?,?,?)}", cli, suc, vino, fecha, cant);
    }

    // 9. Baja Suministros
    public void bajaSuministros(int cli, int suc, int vino, Date fecha) {
        ejecutarProcedimiento("{call pr_Baja_Suministros(?,?,?,?)}", cli, suc, vino, fecha);
    }

    // 10. Alta Pedido Sucursal
    public void altaPedidoSucursal(int sucPide, int sucProv, int vino, Date fecha, int cant) {
        ejecutarProcedimiento("{call pr_Alta_Pedido_Suc(?,?,?,?,?)}", sucPide, sucProv, vino, fecha, cant);
    }

    // 11. Baja Pedido Sucursal
    public void bajaPedidoSucursal(int sucPide, int sucProv, int vino, Date fecha) {
        ejecutarProcedimiento("{call pr_Baja_Pedido_Suc(?,?,?,?)}", sucPide, sucProv, vino, fecha);
    }

    // 12. Alta Vino
    public void altaVino(int cod, String marca, int anio, String den, double grad, String vin, String ca, int cant, int prod) {
        ejecutarProcedimiento("{call pr_Alta_Vino(?,?,?,?,?,?,?,?,?)}", cod, marca, anio, den, grad, vin, ca, cant, prod);
    }

    // 13. Baja Vino
    public void bajaVino(int cod) {
        ejecutarProcedimiento("{call pr_Baja_Vino(?)}", cod);
    }

    // 14. Alta Productor
    public void altaProductor(int cod, String dni, String nom, String dir) {
        ejecutarProcedimiento("{call pr_Alta_Productor(?,?,?,?)}", cod, dni, nom, dir);
    }

    // 15. Baja Productor
    public void bajaProductor(int cod) {
        ejecutarProcedimiento("{call pr_Baja_Productor(?)}", cod);
    }
}