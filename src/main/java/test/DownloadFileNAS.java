package test;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.chilkatsoft.CkGlobal;
import com.chilkatsoft.CkScp;
import com.chilkatsoft.CkSsh;

import connection.ConnectionBD;

public class DownloadFileNAS {
	private PreparedStatement pst = null;
	private ResultSet rs = null;
	private String sql;
	static {
		try {
			System.loadLibrary("chilkat"); // copy file chilkat.dll vao thu muc project
		} catch (UnsatisfiedLinkError e) {
			System.err.println("Native code library failed to load.\n" + e);
			System.exit(1);
		}
	}

	public void childkatDownload(String hostname, int port, String username_scp, String password_scp,
			String syn_must_math, String server_path, String local_path, int mode_scp) {
		CkSsh ssh = new CkSsh();
		CkGlobal ck = new CkGlobal();
		ck.UnlockBundle("Hello");
		boolean success = ssh.Connect(hostname, port);
		if (success != true) {
			System.out.println(ssh.lastErrorText());
			return;
		}

		ssh.put_IdleTimeoutMs(5000);
		success = ssh.AuthenticatePw(username_scp, password_scp);
		if (success != true) {
			System.out.println(ssh.lastErrorText());
			return;
		}
		CkScp scp = new CkScp();

		success = scp.UseSsh(ssh);
		if (success != true) {
			System.out.println(scp.lastErrorText());
			return;
		}
		scp.put_SyncMustMatch(syn_must_math);// down tat ca cac file bat dau bang sinhvien
		success = scp.SyncTreeDownload(server_path, server_path, mode_scp, false);
		if (success != true) {
			System.out.println(scp.lastErrorText());
			return;
		}

		ssh.Disconnect();
	}
	
	public boolean downloadFileOnServer() {
		boolean result = false;
		sql = "SELECT * FROM scp";
		try {
			//1. select fields table SCP in data SCP using download
			pst = new ConnectionBD().getConnection("control_db").prepareStatement(sql);
			rs = pst.executeQuery();
			while (rs.next()) {
				String hostname_scp = rs.getString("hostname_scp");
				int port_scp = rs.getInt("port_scp");
				String username_scp = rs.getString("username_scp");
				String password_scp = rs.getString("password_scp");
				String syn_must_math = rs.getString("syn_must_math");
				String server_path = rs.getString("server_path");
				String local_path = rs.getString("local_path");
				int mode_scp = rs.getInt("mode_scp");
				childkatDownload(hostname_scp, port_scp, username_scp, password_scp, syn_must_math, server_path,
						local_path, mode_scp);
				result = true;
				return result;
			}

		} catch (Exception e) {
			e.printStackTrace();
			return result;

		} finally {
			try {
				if (pst != null)
					pst.close();
				if (rs != null)
					rs.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}

		}

		return result;
	}
	public static void main(String[] args) {
		System.out.println(new DownloadFileNAS().downloadFileOnServer());
	}
}
