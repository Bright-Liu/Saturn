from utils import postgresql

conn = postgresql.getConnection("192.168.20.232", "5432", "wpfuser", "wpf1234", "windpowerdb")
sql = "SELECT log_time, machine_id, ci_nacelle_position, ci_rotor_speed, ci_sub_pcs_active_power, ci_sub_pcs_measured_electrical_torque FROM metadata.t_ai_data"
rows = postgresql.getRows(conn, sql)
for row in rows:
    print(row[0], row[1], row[2], row[3], row[4], row[5])
