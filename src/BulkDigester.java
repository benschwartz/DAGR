import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributeView;
import java.nio.file.attribute.FileOwnerAttributeView;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.FilenameUtils;

public class BulkDigester {

	private static final Logger LOGGER = Logger.getLogger("DAGR");

	private static final Options options;

	private static final DirectoryStream.Filter<Path> directoryFilter = new DirectoryStream.Filter<Path>() {
		@Override
		public boolean accept(Path entry) throws IOException {
			return Files.isDirectory(entry) && !Files.isSymbolicLink(entry);
		}
	};

	private static final DirectoryStream.Filter<Path> fileFilter = new DirectoryStream.Filter<Path>() {
		@Override
		public boolean accept(Path entry) throws IOException {
			return !Files.isDirectory(entry);
		}
	};

	static {
		options = new Options();
		options.addOption("p", true, "path");
		options.addOption("r", false, "recursively descend into directory");
	}

	public static void main(String[] args) {
		CommandLineParser parser = new BasicParser();
		CommandLine cmd = null;
		try {
			cmd = parser.parse(options, args);
			if (!cmd.hasOption("p")) {
				throw new ParseException("");
			}
		} catch (ParseException e) {
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp("java -jar DAGR.jar", options);
			return;
		}
		String parent = Utils.getFileSystemUUID();
		Path top = FileSystems.getDefault().getPath(cmd.getOptionValue("p"));
		if (Files.exists(top)) {
			if (Files.isDirectory(top)) {
				processDirectory(parent, cmd.getOptionValue("p"),
						cmd.hasOption("r"));
			} else {
				processFile(parent, top.toAbsolutePath().toString());
			}
		} else {
			LOGGER.severe("Invalid path: " + cmd.getOptionValue("p"));
		}
	}

	private static void processDirectory(String parent, String path,
			boolean descend) {
		String uuid=insertDirectory(parent, path);
		List<String> files = getFiles(path);
		for (String file : files) {
			processFile(uuid, file);
		}
		if (descend) {
			for (String subdir : getSubdirectories(path)) {
				processDirectory(uuid, subdir, descend);
			}
		}
		else{
			for (String subdir : getSubdirectories(path)){
				insertDirectory(uuid, subdir);
			}
		}

	}
	
	private static String insertDirectory(String parent, String path){
		String uuid = UUID.randomUUID().toString();
		Path directoryPath = FileSystems.getDefault().getPath(path);
		String DAGRName = directoryPath.getFileName().toString();
		BasicFileAttributeView attribs = Files.getFileAttributeView(
				directoryPath, BasicFileAttributeView.class,
				LinkOption.NOFOLLOW_LINKS);
		FileOwnerAttributeView ownerAttribs = Files.getFileAttributeView(
				directoryPath, FileOwnerAttributeView.class,
				LinkOption.NOFOLLOW_LINKS);
		String DAGRType = "directory";
		try {
			long size = Files.size(directoryPath);
			long createTime = attribs.readAttributes().creationTime()
					.toMillis();

			long modifiedTime = attribs.readAttributes().lastModifiedTime()
					.toMillis();
			String owner = ownerAttribs.getOwner().getName();
			String DAGRLocation = directoryPath.getParent().toString();
			/*
			 * LOGGER.info("Processed directory: " + path + "\nname: " +
			 * DAGRName + "\nparent uuid: " + parent + "\nuuid: " + uuid +
			 * "\ntype: " + DAGRType + "\ncreate time: " + createTime +
			 * "\nmodified time: " + modifiedTime + "\nowner: " + owner +
			 * "\nsize: " + size);
			 */
			String result = Utils.insertDAGR(uuid, DAGRName, createTime,
					modifiedTime, DAGRLocation, parent, owner, DAGRType, size);
			if(result != null){
				return result;
			}
			else{
				return null;
			}
		} catch (IOException e) {
			LOGGER.log(Level.SEVERE, "could not get attributes of " + path, e);
			return null;
		}
	}

	private static void processFile(String parent, String file) {
		String uuid = UUID.randomUUID().toString();
		Path filePath = FileSystems.getDefault().getPath(file);
		BasicFileAttributeView attribs = Files.getFileAttributeView(filePath,
				BasicFileAttributeView.class, LinkOption.NOFOLLOW_LINKS);
		FileOwnerAttributeView ownerAttribs = Files.getFileAttributeView(
				filePath, FileOwnerAttributeView.class,
				LinkOption.NOFOLLOW_LINKS);
		String DAGRName = filePath.getFileName().toString();
		String DAGRLocation = filePath.getParent().toString();
		String DAGRType = FilenameUtils.getExtension(filePath.toString());
		try {
			long size = Files.size(filePath);
			Long createTime = attribs.readAttributes().creationTime()
					.toMillis();
			Long modifiedTime = attribs.readAttributes().lastModifiedTime()
					.toMillis();
			String owner = ownerAttribs.getOwner().getName();
			Utils.insertDAGR(uuid, DAGRName, createTime, modifiedTime,
					DAGRLocation, parent, owner, DAGRType, size);
			/*
			 * LOGGER.info("Processed file: " + file + "\nname: " + DAGRName +
			 * "\nparent uuid: " + parent + "\nuuid: " + uuid + "\ntype: " +
			 * DAGRType + "\ncreate time: " + createTime + "\nmodified time: " +
			 * modifiedTime + "\nowner: " + owner + "\nsize: " + size);
			 */
		} catch (IOException e) {
			LOGGER.log(Level.SEVERE, "could not get attributes of " + file, e);
		}
	}

	private static List<String> getFiles(String path) {
		List<String> files = new ArrayList<String>();
		try {
			Path dir = FileSystems.getDefault().getPath(path);
			DirectoryStream<Path> stream = Files.newDirectoryStream(dir,
					fileFilter);
			for (Path p : stream) {
				files.add(p.toAbsolutePath().toString());
			}
			stream.close();
		} catch (IOException e) {
			LOGGER.log(Level.SEVERE, "Could not get the files in " + path, e);
		}
		return files;
	}

	private static List<String> getSubdirectories(String path) {
		List<String> subdirs = new ArrayList<String>();
		try {
			Path dir = FileSystems.getDefault().getPath(path);
			DirectoryStream<Path> stream = Files.newDirectoryStream(dir,
					directoryFilter);
			for (Path p : stream) {
				subdirs.add(p.toAbsolutePath().toString());
			}
			stream.close();
		} catch (IOException e) {
			LOGGER.log(Level.SEVERE, "Could not get subdirectories of " + path,
					e);
		}
		return subdirs;
	}

}
