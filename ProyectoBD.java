import java.sql.*;
/* Grupo 201201
    -- Marco Arjona Núñez
    -- Karl Louis Alfaro
    -- Rym Barkaoui
*/
public class ProyectoBD {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		try {
			/* PARTE 1 */
			
			// String resultado para el procedimiento
			String resultado = null;
			// Procedemos a cargar el driver 
			Class.forName("com.mysql.jdbc.Driver");
			// Creamos conexion a la BD
			Connection connection = DriverManager.getConnection("jdbc:mysql://"+args[0]+":3306/JavaBD?user="+args[1]+"&password="+args[2]);
			Statement sn = connection.createStatement();
			DatabaseMetaData metadata = connection.getMetaData();

			// Para obtener las PK de la tabla pasada como argumento y las propiedades de las columnas
			ResultSet rsetPK = metadata.getPrimaryKeys(null, null, args[3]);
			ResultSet rsetCol = metadata.getColumns(null, null, args[3], null);

			// Contador de columnas
			int n_col = 0;
			while(rsetCol.next()){ 
				n_col++;
				if(rsetCol.isLast()){
					// Imprimimos n_Col y tabla
					resultado = args[3]+"; "+n_col+"; ";
				}
			}
			// Movemos cursor al principio para recorrer nuevamente más adelante
			rsetCol.beforeFirst();

			// Imprimimos PK
			while(rsetPK.next() && !rsetPK.isAfterLast()){ 
				String PK_name = rsetPK.getString("COLUMN_NAME");
				if (rsetPK.isLast())
					resultado = resultado + PK_name+"; ";
				else
					resultado = resultado + PK_name+", ";
			}
			// Imprimimos Atr , TipAtr, LongAtr
			while(rsetCol.next()){
				String nombreColumna = rsetCol.getString("COLUMN_NAME");
				String tipoColumna = rsetCol.getString("TYPE_NAME");
				int tamColumna = rsetCol.getInt("COLUMN_SIZE");
				resultado = resultado + nombreColumna+", "+ tipoColumna + ", "+ tamColumna+"; ";
			}

			// llamamos al procedimiento 
			CallableStatement call = connection.prepareCall("call Proy2012.entrega(9,'"+resultado+"');");
			call.executeQuery();
			
			// Cerramos cursores para no desperdiciar recursos en el SGBD
			call.close();
			rsetCol.close();
			rsetPK.close();
			
			/* ************************************************************************************************************** */

			/* PARTE 2 */
			
			// Consultas SQL
			String sql1 = "select max(store_id) from sakila.store";
			String sql2 = 
					"select max(address_id) from sakila.address where city_id = (select min(city_id) from sakila.city where country_id = (select country_id from sakila.country where country='spain'));";
			
			
			ResultSet rset_sql1 = sn.executeQuery(sql1);			
			rset_sql1.first();
			// Tenemos el id de una Tienda cualquiera
			int idTienda = rset_sql1.getInt(1);
			
			ResultSet rset_sql2 = sn.executeQuery(sql2);
			rset_sql2.first();
			// Tenemos el id de una Ciudad Española (cualquiera)
			int idAddress = rset_sql2.getInt(1);
			
			
			// Consulta SQL para la inserción 
			String sql3 = "insert into sakila.customer(store_id, first_name, last_name, email, address_id, active,create_date) " +
					  "values("+ idTienda +", 'alvaro', 'fernandez', 'al.fernandez@alumnos.upm.es',"+ idAddress +", TRUE, '2012/12/13');";
			Statement st = connection.createStatement();
			int val = st.executeUpdate(sql3);
			System.out.println(val + " rows affected");
			
			// Cerramos cursores
			st.close();
			rset_sql2.close();
			rset_sql1.close();
			
			// Cerramos conexiones
			sn.close();
			connection.close();
			
			// Código de tratamiento de excepciones
		} catch (ClassNotFoundException e) { 
			// TODO Auto-generated catch block
			System.out.println("Error al cargar el driver");
			System.out.println(e.getMessage());
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			System.out.println("Error en la BD");
			System.out.println(e.getMessage());
			e.printStackTrace();
		}
	}
}
