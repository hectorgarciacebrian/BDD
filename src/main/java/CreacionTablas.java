import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class CreacionTablas {

    public static void main(String[] args) {
        DatabaseManager db = DatabaseManager.getInstance();

        // 1. LISTA DE TABLAS A BORRAR
        String[] drops = {
            "DROP TABLE Pide CASCADE CONSTRAINTS",
            "DROP TABLE Solicita CASCADE CONSTRAINTS",
            "DROP TABLE Suministra CASCADE CONSTRAINTS",
            "DROP TABLE Cliente CASCADE CONSTRAINTS",
            "DROP TABLE Empleado CASCADE CONSTRAINTS",
            "DROP TABLE Vino CASCADE CONSTRAINTS",
            "DROP TABLE Productor CASCADE CONSTRAINTS",
            "DROP TABLE Sucursal CASCADE CONSTRAINTS"
        };

        // 2. LISTA DE TABLAS A CREAR
        String[] creates = {
            // Entidad: SUCURSAL
            """
            CREATE TABLE Sucursal (
                cod_sucursal NUMBER(8) NOT NULL,
                nombre       VARCHAR2(50),
                ciudad       VARCHAR2(50),
                pedido       NUMBER(8),
                c_autonoma   VARCHAR2(50),
                director     NUMBER(8)
            )
            """,
            // Entidad: PRODUCTOR
            """
            CREATE TABLE Productor (
                cod_p       NUMBER(8) NOT NULL,
                dni_p       VARCHAR2(9) NOT NULL,
                nombre_p    VARCHAR2(50),
                direccion_p VARCHAR2(100)
            )
            """,
            // Entidad: VINO
            """
            CREATE TABLE Vino (
                cod_vino     NUMBER(8) NOT NULL,
                stock        NUMBER(8) DEFAULT 0,
                vinedo       VARCHAR2(50), 
                c_autonoma   VARCHAR2(50),
                c_producida  NUMBER(8,2), 
                productor    NUMBER(8) NOT NULL 
            )
            """,
            // Entidad: EMPLEADO
            """
            CREATE TABLE Empleado (
                cod_e        NUMBER(8) NOT NULL,
                dni_e        VARCHAR2(9) NOT NULL,
                nombre_e     VARCHAR2(50),
                direccion_e  VARCHAR2(100),
                fecha_comp   DATE,
                salario      NUMBER(8,2),
                sucursal_dest NUMBER(8) NOT NULL
            )
            """,
            // Entidad: CLIENTE
            """
            CREATE TABLE Cliente (
                cod_c       NUMBER(8) NOT NULL,
                dni_c       VARCHAR2(9) NOT NULL,
                nombre_c    VARCHAR2(50),
                direccion_c VARCHAR2(100),
                tipo_c      VARCHAR2(10),
                suministro  VARCHAR2(50),
                c_autonoma  VARCHAR2(50)
            )
            """,
            // Relación: SUMINISTRA
            """
            CREATE TABLE Suministra (
                fecha_su     DATE NOT NULL,
                cod_vino     NUMBER(8) NOT NULL,
                cod_sucursal NUMBER(8) NOT NULL
                
            )
            """,
            // Relación: SOLICITA
            """
            CREATE TABLE Solicita (
                fecha_sol    DATE NOT NULL,
                cod_sucursal NUMBER(8) NOT NULL,
                cod_sucursal_prov NUMBER(8) NOT NULL,
                cod_tipo_vino NUMBER(8) NOT NULL,
                cantidad     NUMBER(8)
            )
            """,
            // Relación: PIDE
            """
            CREATE TABLE Pide (
                fecha_pide   DATE NOT NULL,
                cod_vino     NUMBER(8) NOT NULL,
                cod_sucursal NUMBER(8) NOT NULL,
                cod_cliente  NUMBER(8) NOT NULL,
                cantidad     NUMBER(8)
            )
            """
        };

        // 3. RESTRICCIONES 
        String[] constraints = {
            // --- PRIMARY KEYS ---
            "ALTER TABLE Sucursal ADD CONSTRAINT PK_Sucursal PRIMARY KEY (cod_sucursal)",
            "ALTER TABLE Productor ADD CONSTRAINT PK_Productor PRIMARY KEY (cod_p)",
            "ALTER TABLE Vino ADD CONSTRAINT PK_Vino PRIMARY KEY (cod_vino)",
            "ALTER TABLE Empleado ADD CONSTRAINT PK_Empleado PRIMARY KEY (cod_e)",
            "ALTER TABLE Cliente ADD CONSTRAINT PK_Cliente PRIMARY KEY (cod_c)",
            
            // PKs Compuestas para las relaciones
            "ALTER TABLE Suministra ADD CONSTRAINT PK_Suministra PRIMARY KEY (fecha_su, cod_vino, cod_sucursal)",
           "ALTER TABLE Solicita ADD CONSTRAINT PK_Solicita PRIMARY KEY (fecha_sol, cod_sucursal, cod_sucursal_prov, cod_tipo_vino)",
            "ALTER TABLE Pide ADD CONSTRAINT PK_Pide PRIMARY KEY (fecha_pide, cod_vino, cod_sucursal, cod_cliente)",

            // --- UNIQUE KEYS ---
            "ALTER TABLE Productor ADD CONSTRAINT UQ_Productor_DNI UNIQUE (dni_p)",
            "ALTER TABLE Empleado ADD CONSTRAINT UQ_Empleado_DNI UNIQUE (dni_e)",
            "ALTER TABLE Cliente ADD CONSTRAINT UQ_Cliente_DNI UNIQUE (dni_c)",

            // --- FOREIGN KEYS  ---
            // Vino apunta a Productor
            "ALTER TABLE Vino ADD CONSTRAINT FK_Vino_Productor FOREIGN KEY (productor) REFERENCES Productor (cod_p)",
          
            // Relaciones N:M
            "ALTER TABLE Suministra ADD CONSTRAINT FK_Sum_Vino FOREIGN KEY (cod_vino) REFERENCES Vino (cod_vino)",
            "ALTER TABLE Suministra ADD CONSTRAINT FK_Sum_Suc FOREIGN KEY (cod_sucursal) REFERENCES Sucursal (cod_sucursal)",
            
            "ALTER TABLE Pide ADD CONSTRAINT FK_Pide_Vino FOREIGN KEY (cod_vino) REFERENCES Vino (cod_vino)",
            "ALTER TABLE Pide ADD CONSTRAINT FK_Pide_Suc FOREIGN KEY (cod_sucursal) REFERENCES Sucursal (cod_sucursal)",
            "ALTER TABLE Pide ADD CONSTRAINT FK_Pide_Clien FOREIGN KEY (cod_cliente) REFERENCES Cliente (cod_c)",
            
            "ALTER TABLE Solicita ADD CONSTRAINT FK_Solicita_Suc_Pide FOREIGN KEY (cod_sucursal) REFERENCES Sucursal (cod_sucursal)",
            "ALTER TABLE Solicita ADD CONSTRAINT FK_Solicita_Suc_Prov FOREIGN KEY (cod_sucursal_prov) REFERENCES Sucursal (cod_sucursal)",
            "ALTER TABLE Solicita ADD CONSTRAINT FK_Solicita_Vino     FOREIGN KEY (cod_tipo_vino) REFERENCES Vino (cod_vino)",

            // Director de Sucursal 
            "ALTER TABLE Sucursal ADD CONSTRAINT FK_Sucursal_Director FOREIGN KEY (director) REFERENCES Empleado (cod_e)"
        };

        System.out.println("--- CREACIÓN DE TABLAS ---");

        for (DatabaseManager.Delegacion d : DatabaseManager.Delegacion.values()) {
            System.out.println("\nNodo: " + d);
            
            try (Connection conn = db.getConnection(d);
                 Statement stmt = conn.createStatement()) {

                // 1. Borrar tablas viejas
                System.out.print("Limpiando... ");
                for (String sql : drops) {
                    try { stmt.executeUpdate(sql); } catch (SQLException ignored) {}
                }
                System.out.println("OK.");

                // 2. Crear tablas nuevas
                System.out.print("Creando estructura... ");
                for (String sql : creates) {
                    stmt.executeUpdate(sql);
                }
                System.out.println("OK.");

                // 3. Poner restricciones
                System.out.print("Conectando claves... ");
                for (String sql : constraints) {
                    stmt.executeUpdate(sql);
                }
                System.out.println("OK.");

            } catch (SQLException e) {
                System.err.println("ERROR EN " + d + ": " + e.getMessage());
            }
        }
        System.out.println("\nSISTEMA DESPLEGADO CORRECTAMENTE.");
    }
}