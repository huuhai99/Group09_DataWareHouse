package test;

import java.util.Properties;

import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.PasswordAuthentication;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;

import com.chilkatsoft.CkGlobal;
import com.chilkatsoft.CkScp;
import com.chilkatsoft.CkSsh;

public class DownloadFileNAS {
	static {
		try {
			System.loadLibrary("chilkat"); // copy file chilkat.dll vao thu muc project
		} catch (UnsatisfiedLinkError e) {
			System.err.println("Native code library failed to load.\n" + e);
			System.exit(1);
		}
	}
	public void downloadSCP() {
		CkSsh ssh = new CkSsh();
		CkGlobal ck = new CkGlobal();
		ck.UnlockBundle("Hello");
		String hostname = "drive.ecepvn.org";
		int port = 2227;
		boolean success = ssh.Connect(hostname, port);
		if (success != true) {
			System.out.println(ssh.lastErrorText());
			return;
		}

		ssh.put_IdleTimeoutMs(5000);
		success = ssh.AuthenticatePw("guest_access", "123456");
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
		scp.put_SyncMustMatch("sinhvien*.*");// down tat ca cac file bat dau bang sinhvien
		String remotePath = "/volume1/ECEP/song.nguyen/DW_2020/data";
		String localPath = "D:\\LAB_DW"; // thu muc muon down file ve
		success = scp.SyncTreeDownload(remotePath, localPath, 2, false);
		System.out.println("Download file success!");
		if (success != true) {
			System.out.println(scp.lastErrorText());
			return;
		}

		ssh.Disconnect();
	}
	public boolean isDownLoadSuccess() {
		downloadSCP();
		return true;
	}
	public static void main(String argv[]) {
		DownloadFileNAS dfn = new DownloadFileNAS();
		dfn.downloadSCP();
	}
	public static boolean sendMail(String to, String subject, String bodyMail, String mail, String pass) {
		Properties props = new Properties();
		props.put("mail.smtp.auth", "true");
		props.put("mail.smtp.starttls.enable", "true");
		props.put("mail.smtp.host", "smtp.gmail.com");
		props.put("mail.smtp.port", "587");
		Session session = Session.getInstance(props, new javax.mail.Authenticator() {
			@Override
			protected PasswordAuthentication getPasswordAuthentication() {
				return new PasswordAuthentication(mail, pass);
			}
		});
		try {
			MimeMessage message = new MimeMessage(session);
			message.setHeader("Content-Type", "text/plain; charset=UTF-8");
			message.setFrom(new InternetAddress("ngochaipham07@gmail.com"));
			message.setRecipients(Message.RecipientType.TO, InternetAddress.parse(to));
			message.setSubject(subject, "UTF-8");
			message.setText(bodyMail, "UTF-8");
			Transport.send(message);
		} catch (MessagingException e) {
			return false;
		}
		return true;
	}
}
