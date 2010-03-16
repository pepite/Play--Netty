package controllers;

import org.apache.commons.io.FileUtils;
import play.Play;
import play.mvc.*;
import play.Logger;

import java.io.File;

public class Application extends Controller {

    public static void index() {
        render();
    }

    public static void upload(File file) throws Exception {
        Logger.info("File is " + file.getAbsolutePath() + "]");
        FileUtils.copyFile(file, new File(Play.getFile("data/"), file.getName()));
        Logger.info("File size [" + file.length() + "]");
    }

}