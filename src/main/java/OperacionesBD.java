import java.sql.*;

public class OperacionesBD {

    private DatabaseManager db;
    private DatabaseManager.Delegacion delegacionActual;

    public OperacionesBD(DatabaseManager.Delegacion delegacion) {
        this.db = DatabaseManager.getInstance();
        this.delegacionActual = delegacion;
    }

    // --- MÉTODOS AUXILIARES ---
    
    // Método para ejecutar actualizaciones simples con manejo de errores
    private void ejecutarUpdate(String sql, Object... params) {
        try (Connection conn = db.getConnection(delegacionActual);
             PreparedStatement ps = conn.prepareStatement(sql)) {
            
            for (int i = 0; i < params.length; i++) {
                if (params[i] == null) {
                    ps.setNull(i + 1, Types.VARCHAR); // Asumimos VARCHAR por defecto para nulos genéricos
                } else if (params[i] instanceof String) {
                    ps.setString(i + 1, (String) params[i]);
                } else if (params[i] instanceof Integer) {
                    ps.setInt(i + 1, (Integer) params[i]);
                } else if (params[i] instanceof Double) {
                    ps.setDouble(i + 1, (Double) params[i]);
                } else if (params[i] instanceof java.sql.Date) {
                    ps.setDate(i + 1, (java.sql.Date) params[i]);
                } else {
                    ps.setObject(i + 1, params[i]);
                }
            }
            int filas = ps.executeUpdate();
            System.out.println("Operación realizada. Filas afectadas: " + filas);

        } catch (SQLException e) {
            System.err.println("ERROR SQL: " + e.getMessage());
            // Aquí se mostrarán los errores de los Triggers (-20000)
        }
    }

    // --- OPERACIONES (1-15) ---

    // 1. Dar de alta a un nuevo empleado
    public void altaEmpleado(int cod, String dni, String nombre, Date fechaInicio, double salario, String direccion, int codSucursal) {
        String sql = "INSERT INTO Empleado (cod_e, dni_e, nombre_e, fecha_comp, salario, direccion_e, sucursal_dest) VALUES (?, ?, ?, ?, ?, ?, ?)";
        ejecutarUpdate(sql, cod, dni, nombre, fechaInicio, salario, direccion, codSucursal);
    }

    // 2. Dar de baja a un empleado (Gestión de Director)
    public void bajaEmpleado(int codEmpleado) {
        // Primero verificamos si es director y lo desvinculamos para evitar error FK
        String sqlUpdateDirector = "UPDATE Sucursal SET director = NULL WHERE director = ?";
        String sqlDelete = "DELETE FROM Empleado WHERE cod_e = ?";
        
        try (Connection conn = db.getConnection(delegacionActual)) {
            conn.setAutoCommit(false); // Transacción
            try {
                // 1. Desvincular de sucursales
                try (PreparedStatement psDir = conn.prepareStatement(sqlUpdateDirector)) {
                    psDir.setInt(1, codEmpleado);
                    psDir.executeUpdate();
                }
                
                // 2. Borrar empleado
                try (PreparedStatement psDel = conn.prepareStatement(sqlDelete)) {
                    psDel.setInt(1, codEmpleado);
                    int filas = psDel.executeUpdate();
                    if (filas == 0) System.out.println("No se encontró el empleado con código " + codEmpleado);
                }
                
                conn.commit();
                System.out.println("Baja de empleado realizada correctamente.");
            } catch (SQLException ex) {
                conn.rollback();
                System.err.println("Error al dar de baja empleado: " + ex.getMessage());
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    // 3. Modificar el salario de un empleado
    public void modificarSalario(int codEmpleado, double nuevoSalario) {
        String sql = "UPDATE Empleado SET salario = ? WHERE cod_e = ?";
        ejecutarUpdate(sql, nuevoSalario, codEmpleado);
    }

    // 4. Trasladar de sucursal a un empleado
    public void trasladarEmpleado(int codEmpleado, int codNuevaSucursal, String nuevaDireccion) {
        // Si nuevaDireccion es null, SQL debe poner NULL (manejado en ejecutarUpdate)
        String sql = "UPDATE Empleado SET sucursal_dest = ?, direccion_e = ? WHERE cod_e = ?";
        ejecutarUpdate(sql, codNuevaSucursal, nuevaDireccion, codEmpleado);
    }

    // 5. Dar de alta una nueva sucursal
    public void altaSucursal(int codSucursal, String nombre, String ciudad, String cAutonoma, Integer codDirector) {
        String sql = "INSERT INTO Sucursal (cod_sucursal, nombre, ciudad, c_autonoma, director) VALUES (?, ?, ?, ?, ?)";
        ejecutarUpdate(sql, codSucursal, nombre, ciudad, cAutonoma, codDirector);
    }

    // 6. Cambiar el director de una sucursal
    public void cambiarDirector(int codSucursal, int codNuevoDirector) {
        String sql = "UPDATE Sucursal SET director = ? WHERE cod_sucursal = ?";
        ejecutarUpdate(sql, codNuevoDirector, codSucursal);
    }

    // 7. Dar de alta a un nuevo cliente
    public void altaCliente(int codCliente, String dni, String nombre, String direccion, String tipo, String cAutonoma) {
        String sql = "INSERT INTO Cliente (cod_c, dni_c, nombre_c, direccion_c, tipo_c, c_autonoma) VALUES (?, ?, ?, ?, ?, ?)";
        ejecutarUpdate(sql, codCliente, dni, nombre, direccion, tipo, cAutonoma);
    }

    // 8. Dar de alta o actualizar un suministro (Tabla Pide) + Actualización Stock
    public void gestionarSuministro(int codCliente, int codSucursal, int codVino, Date fecha, int cantidad) {
        // Lógica: Check if exists -> Update or Insert. Then Update Stock.
        String checkSql = "SELECT cantidad FROM Pide WHERE cod_cliente=? AND cod_sucursal=? AND cod_vino=? AND fecha_pide=?";
        String insertSql = "INSERT INTO Pide (cod_cliente, cod_sucursal, cod_vino, fecha_pide, cantidad) VALUES (?, ?, ?, ?, ?)";
        String updateSql = "UPDATE Pide SET cantidad = cantidad + ? WHERE cod_cliente=? AND cod_sucursal=? AND cod_vino=? AND fecha_pide=?";
        String stockSql  = "UPDATE Vino SET stock = stock - ? WHERE cod_vino = ?";

        try (Connection conn = db.getConnection(delegacionActual)) {
            conn.setAutoCommit(false);
            try {
                boolean existe = false;
                try (PreparedStatement psCheck = conn.prepareStatement(checkSql)) {
                    psCheck.setInt(1, codCliente); psCheck.setInt(2, codSucursal);
                    psCheck.setInt(3, codVino); psCheck.setDate(4, fecha);
                    ResultSet rs = psCheck.executeQuery();
                    if (rs.next()) existe = true;
                }

                if (existe) {
                    try (PreparedStatement psUpd = conn.prepareStatement(updateSql)) {
                        psUpd.setInt(1, cantidad);
                        psUpd.setInt(2, codCliente); psUpd.setInt(3, codSucursal);
                        psUpd.setInt(4, codVino); psUpd.setDate(5, fecha);
                        psUpd.executeUpdate();
                    }
                } else {
                    try (PreparedStatement psIns = conn.prepareStatement(insertSql)) {
                        psIns.setInt(1, codCliente); psIns.setInt(2, codSucursal);
                        psIns.setInt(3, codVino); psIns.setDate(4, fecha);
                        psIns.setInt(5, cantidad);
                        psIns.executeUpdate();
                    }
                }

                // Actualizar Stock
                try (PreparedStatement psStock = conn.prepareStatement(stockSql)) {
                    psStock.setInt(1, cantidad); // Si cantidad es negativa (devolución), stock aumenta automáticamente
                    psStock.setInt(2, codVino);
                    psStock.executeUpdate();
                }

                conn.commit();
                System.out.println("Suministro gestionado y stock actualizado.");
            } catch (SQLException ex) {
                conn.rollback();
                System.err.println("Error en suministro: " + ex.getMessage());
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    // 9. Dar de baja suministros + Restaurar Stock
    public void bajaSuministros(int codCliente, int codSucursal, int codVino, Date fechaOpcional) {
        String selectQta;
        String deleteSql;
        
        if (fechaOpcional != null) {
            selectQta = "SELECT cantidad FROM Pide WHERE cod_cliente=? AND cod_sucursal=? AND cod_vino=? AND fecha_pide=?";
            deleteSql = "DELETE FROM Pide WHERE cod_cliente=? AND cod_sucursal=? AND cod_vino=? AND fecha_pide=?";
        } else {
            selectQta = "SELECT cantidad FROM Pide WHERE cod_cliente=? AND cod_sucursal=? AND cod_vino=?";
            deleteSql = "DELETE FROM Pide WHERE cod_cliente=? AND cod_sucursal=? AND cod_vino=?";
        }
        
        String stockSql = "UPDATE Vino SET stock = stock + ? WHERE cod_vino = ?";

        try (Connection conn = db.getConnection(delegacionActual)) {
            conn.setAutoCommit(false);
            try {
                int totalCantidadRestaurar = 0;
                
                // 1. Calcular cantidad a restaurar al stock antes de borrar
                try (PreparedStatement psSel = conn.prepareStatement(selectQta)) {
                    psSel.setInt(1, codCliente); psSel.setInt(2, codSucursal); psSel.setInt(3, codVino);
                    if (fechaOpcional != null) psSel.setDate(4, fechaOpcional);
                    
                    ResultSet rs = psSel.executeQuery();
                    while(rs.next()) {
                        totalCantidadRestaurar += rs.getInt("cantidad");
                    }
                }

                if (totalCantidadRestaurar > 0) {
                    // 2. Restaurar Stock
                    try (PreparedStatement psStk = conn.prepareStatement(stockSql)) {
                        psStk.setInt(1, totalCantidadRestaurar);
                        psStk.setInt(2, codVino);
                        psStk.executeUpdate();
                    }

                    // 3. Borrar registros
                    try (PreparedStatement psDel = conn.prepareStatement(deleteSql)) {
                        psDel.setInt(1, codCliente); psDel.setInt(2, codSucursal); psDel.setInt(3, codVino);
                        if (fechaOpcional != null) psDel.setDate(4, fechaOpcional);
                        psDel.executeUpdate();
                    }
                    
                    conn.commit();
                    System.out.println("Suministro eliminado y stock restaurado (" + totalCantidadRestaurar + ").");
                } else {
                    System.out.println("No se encontraron suministros para dar de baja.");
                    conn.rollback();
                }
            } catch (SQLException ex) {
                conn.rollback();
                System.err.println("Error al dar de baja suministro: " + ex.getMessage());
            }
        } catch (SQLException e) { e.printStackTrace(); }
    }

    // 10. Dar de alta un pedido de una sucursal (Tabla Solicita) + Actualización Stock
    public void altaPedidoSucursal(int codSucursalPide, int codSucursalProv, int codVino, Date fecha, int cantidad) {
        // Nota: En tabla Solicita, el campo de vino se llama 'cod_tipo_vino'
        String insertSql = "INSERT INTO Solicita (cod_sucursal, cod_sucursal_prov, cod_tipo_vino, fecha_sol, cantidad) VALUES (?, ?, ?, ?, ?)";
        // Asumimos que un pedido entre sucursales TAMBIÉN reduce el stock global/productor del vino (Nota 2)
        String stockSql = "UPDATE Vino SET stock = stock - ? WHERE cod_vino = ?";

        try (Connection conn = db.getConnection(delegacionActual)) {
            conn.setAutoCommit(false);
            try {
                try (PreparedStatement psIns = conn.prepareStatement(insertSql)) {
                    psIns.setInt(1, codSucursalPide);
                    psIns.setInt(2, codSucursalProv);
                    psIns.setInt(3, codVino);
                    psIns.setDate(4, fecha);
                    psIns.setInt(5, cantidad);
                    psIns.executeUpdate();
                }
                
                try (PreparedStatement psStk = conn.prepareStatement(stockSql)) {
                    psStk.setInt(1, cantidad);
                    psStk.setInt(2, codVino);
                    psStk.executeUpdate();
                }

                conn.commit();
                System.out.println("Pedido de sucursal realizado.");
            } catch (SQLException ex) {
                conn.rollback();
                System.err.println("Error en pedido sucursal: " + ex.getMessage());
            }
        } catch (SQLException e) { e.printStackTrace(); }
    }
}