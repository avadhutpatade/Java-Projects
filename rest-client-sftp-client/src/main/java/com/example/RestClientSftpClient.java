package com.example;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;

/**
 * @author Avadhut.Patade
 *
 */
public class RestClientSftpClient {

	private static final Logger logger = LogManager.getLogger(RestClientSftpClient.class);

	static {
		LoggerContext context = (LoggerContext) LogManager.getContext(false);
		File file = new File("log4j2.xml");
		context.setConfigLocation(file.toURI());
	}

	private static final Properties properties = new Properties();

	static {
		try (InputStream inputStream = new FileInputStream("./config.properties")) {
			properties.load(inputStream);
			logger.info("Properties Loading Success: {}", properties);
		} catch (Exception e) {
			logger.error("Properties Loading Failed: \n\n {}", ExceptionUtils.getStackTrace(e));
		}
	}

	private static final String PROTOCOL_TYPE = properties.getProperty("protocol.type");
	private static final String HOST_ADDRESS = properties.getProperty("host.address");
	private static final String HOST_PORT = properties.getProperty("host.port");
	private static final String CLIENT_ID = properties.getProperty("client.id");
	private static final String CLIENT_SECRET = properties.getProperty("client.secret");
	private static final String USER_AUTHENTICATION_USERNAME = properties.getProperty("user.authentication.username");
	private static final String USER_AUTHENTICATION_PASSWORD = properties.getProperty("user.authentication.password");
	private static final String USER_AUTHENTICATION_GRANT_TYPE = properties.getProperty("user.authentication.grant.type");
	private static final String ACCESS_TOKEN_ENDPOINT = properties.getProperty("access.token.endpoint");
	private static final String DATA_FIRST_ENDPOINT = properties.getProperty("data.first.endpoint");
	private static final String DATA_SECOND_ENDPOINT = properties.getProperty("data.second.endpoint");

	private static final String SFTP_SERVER_HOST_ADDRESS = properties.getProperty("sftp.server.host.address");
	private static final Integer SFTP_SERVER_HOST_PORT = Integer.parseInt(properties.getProperty("sftp.server.host.port"));
	private static final String SFTP_SERVER_AUTHENTICATION_USERNAME = properties.getProperty("sftp.server.authentication.username");
	private static final String SFTP_SERVER_AUTHENTICATION_PASSWORD = properties.getProperty("sftp.server.authentication.password");
	private static final String SFTP_SERVER_DIRECTORY_ADDRESS = properties.getProperty("sftp.server.directory.address");

	private static final String AUTHORIZATION = "Authorization";

	private static String token = null;
	private static ObjectMapper objectMapper = new ObjectMapper();
	private static List<String> filenamesWrittenOnSftp = new ArrayList<>();

	public static void main(String[] args) {
		getAndIntializeToken();
		if (token != null) {
			List<Map<String, Integer>> dataRecords = null;
			if (args.length > 0) {
				String filename = args[0];
				dataRecords = getDataRecordsUsingHardCodeFile(filename);
			} else {
				dataRecords = getDataRecordsUsingAPI();
			}
			String dateStr = getDateStr();
			for (Map<String, Integer> dataRecord : dataRecords) {
				logger.info("---------------------------------------------------------------------------------");
				Map<String, Object> data = getData(dataRecord);
				if (data != null) {
					File localFile = createFileOnLocalSystem(data, dateStr);
					if (localFile != null) {
						writeFileOnSftpServer(localFile);
						deleteCreatedFileOnLocalSystem(localFile);
					}
				}
			}
		}
		logger.info("---------------------------------------------------------------------------------");
		logger.info("Files written on SFTP server are: {}", filenamesWrittenOnSftp);
	}

	private static void deleteCreatedFileOnLocalSystem(File localFile) {
		if (localFile.delete()) {
			logger.info("Local System File Deletion Success: {}", localFile.getName());
		} else {
			logger.error("Local System File Deletion Failure: {}", localFile.getName());
		}
	}

	private static void writeFileOnSftpServer(File localFile) {
		Session session = null;
		Channel channel = null;
		try {
			JSch jsch = new JSch();
			session = jsch.getSession(SFTP_SERVER_AUTHENTICATION_USERNAME, SFTP_SERVER_HOST_ADDRESS,
					SFTP_SERVER_HOST_PORT);
			session.setPassword(SFTP_SERVER_AUTHENTICATION_PASSWORD);
			java.util.Properties config = new java.util.Properties();
			config.put("StrictHostKeyChecking", "no");
			session.setConfig(config);
			session.connect();
			channel = session.openChannel("sftp");
			channel.connect();
			ChannelSftp channelSftp = (ChannelSftp) channel;
			String[] folders = SFTP_SERVER_DIRECTORY_ADDRESS.split("/");
			for (String folder : folders) {
				if (folder.length() > 0) {
					makeDirectoriesOnSftpServerIfNotPresent(channelSftp, folder);
				}
			}
			FileInputStream fileInputStream = new FileInputStream(localFile);
			channelSftp.put(fileInputStream, localFile.getName());
			fileInputStream.close();
			channelSftp.exit();
			logger.info("SFTP Server File Writing Success: {}", localFile);
			filenamesWrittenOnSftp.add(localFile.getName());
		} catch (Exception e) {
			logger.error("SFTP Server File Writing Failure: \n\n {}", ExceptionUtils.getStackTrace(e));
		} finally {
			channel.disconnect();
			session.disconnect();
		}
	}

	private static void makeDirectoriesOnSftpServerIfNotPresent(ChannelSftp channelSftp, String folder)
			throws SftpException {
		try {
			channelSftp.cd(folder);
		} catch (SftpException e) {
			channelSftp.mkdir(folder);
			channelSftp.cd(folder);
		}
	}

	private static File createFileOnLocalSystem(Map<String, Object> data, String dateStr) {
		File localFile = null;
		try {
			String fileName = data.get("firstName") + "_" + data.get("secondName") + "_" + data.get("uniqueName") + "_"
					+ dateStr + ".json";
			localFile = new File(fileName);
			objectMapper.writeValue(localFile, data);
			logger.info("Local System File Creation Success: {}", fileName);
		} catch (Exception e) {
			logger.error("Local System File Creation Failed: \n\n {}", ExceptionUtils.getStackTrace(e));
		}
		return localFile;
	}

	private static Map<String, Object> getData(Map<String, Integer> dataRecord) {
		try {
			String urlString = new StringBuilder(PROTOCOL_TYPE).append(HOST_ADDRESS).append(HOST_PORT)
					.append(DATA_SECOND_ENDPOINT).toString();
			URL url = new URL(urlString);

			HttpURLConnection connection = (HttpURLConnection) url.openConnection();
			connection.setRequestMethod("GET");
			connection.setDoOutput(true);
			connection.setRequestProperty(AUTHORIZATION, "Bearer " + token);

			@SuppressWarnings("unchecked")
			Map<String, Object> data = objectMapper.readValue(connection.getInputStream(), Map.class);
			connection.disconnect();
			logger.info("Data Fetch Success for Data Record: {}", dataRecord);
			return data;
		} catch (Exception e) {
			logger.info("Data Fetch Failure for Data Record: {} \n\n {}", dataRecord, ExceptionUtils.getStackTrace(e));
		}
		return null;
	}

	private static String getDateStr() {
		// Getting todays date, formatted as MMddyyyy
		Date date = Calendar.getInstance().getTime();
		DateFormat dateFormat = new SimpleDateFormat("MMddyyy");
		String dateStr = dateFormat.format(date);
		return dateStr;
	}

	private static List<Map<String, Integer>> getDataRecordsUsingAPI() {
		try {
			// http://hostAddress:port/dataFirstEndpoint
			StringBuilder urlStringBuilder = new StringBuilder(PROTOCOL_TYPE).append(HOST_ADDRESS).append(HOST_PORT)
					.append(DATA_FIRST_ENDPOINT);
			URL url = new URL(urlStringBuilder.toString());

			HttpURLConnection connection = (HttpURLConnection) url.openConnection();
			connection.setRequestMethod("GET");
			connection.setDoOutput(true);
			connection.setRequestProperty(AUTHORIZATION, "Bearer " + token);

			@SuppressWarnings("unchecked")
			List<Map<String, Integer>> dataRecords = objectMapper.readValue(connection.getInputStream(), List.class);
			connection.disconnect();
			logger.info("Data Records Fetching Using API Success: {}", dataRecords);
			return dataRecords;
		} catch (Exception e) {
			logger.error("Data Records Fetching Using API Failure: \n\n {}", ExceptionUtils.getStackTrace(e));
		}
		return new ArrayList<>();
	}

	private static List<Map<String, Integer>> getDataRecordsUsingHardCodeFile(String filename) {
		List<Map<String, Integer>> dataRecords = new ArrayList<>();
		try {
			File firstTimeFile = new File(filename);
			BufferedReader bufferedReader = new BufferedReader(new FileReader(firstTimeFile));
			String curRecord;
			while ((curRecord = bufferedReader.readLine()) != null) {
				Map<String, Integer> dataRecord = new HashMap<>();
				// convert curRecord as required and write into dataRecord with corresponding keys
				curRecord.split("\t");
				dataRecords.add(dataRecord);
			}
			bufferedReader.close();
			logger.info("Data Records Fetching Using Hard-Coded File Success: {}", dataRecords);
		} catch (Exception e) {
			logger.error("Data Records Fetching Using Hard-Coded File Failure: \n\n {}",
					ExceptionUtils.getStackTrace(e));
		}
		return dataRecords;
	}

	private static void getAndIntializeToken() {
		try {
			// http://hostAddress:port/tokenEndpoint?username=username&password=password&grant_type=grant_type
			StringBuilder urlStringBuilder = new StringBuilder(PROTOCOL_TYPE).append(HOST_ADDRESS).append(HOST_PORT)
					.append(ACCESS_TOKEN_ENDPOINT).append("?").append("username=").append(USER_AUTHENTICATION_USERNAME)
					.append("&").append("password=").append(USER_AUTHENTICATION_PASSWORD).append("&")
					.append("grant_type=").append(USER_AUTHENTICATION_GRANT_TYPE);
			URL url = new URL(urlStringBuilder.toString());
			String encoding = Base64.getEncoder().encodeToString((CLIENT_ID + ":" + CLIENT_SECRET).getBytes());

			HttpURLConnection connection = (HttpURLConnection) url.openConnection();
			connection.setRequestMethod("POST");
			connection.setDoOutput(true);
			connection.setRequestProperty(AUTHORIZATION, "Basic " + encoding);

			@SuppressWarnings("unchecked")
			Map<String, Object> response = objectMapper.readValue(connection.getInputStream(), Map.class);
			token = (String) response.get("access_token");
			connection.disconnect();
			logger.info("Token Genaration Success: {}", token);
		} catch (Exception e) {
			logger.error("Token Genaration Failed: \n\n {}", ExceptionUtils.getStackTrace(e));
		}
	}
}
