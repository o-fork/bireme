package cn.hashdata.bireme;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.daemon.DaemonInitException;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

/**
 * @author: : yangyang.li
 * Created time: 2018/11/28.
 * Copyright(c) TTPai All Rights Reserved.
 * Description :
 */
public class Test {


    public static void main(String[] args) {

        Option help = new Option("help", "print this message");
        Option configFile =
                Option.builder("config_file").hasArg().argName("file").desc("config file location").build();

        Options opts = new Options();
        opts.addOption(help);
        opts.addOption(configFile);
        CommandLine cmd = null;
        CommandLineParser parser = new DefaultParser();

        try {
            cmd = parser.parse(opts, args);

            if (cmd.hasOption("help")) {
                throw new ParseException("print help message");
            }
        } catch (ParseException e) {
            HelpFormatter formatter = new HelpFormatter();
            StringWriter out = new StringWriter();
            PrintWriter writer = new PrintWriter(out);
            formatter.printHelp(writer, formatter.getWidth(), "Bireme", null, opts,
                    formatter.getLeftPadding(), formatter.getDescPadding(), null, true);
            writer.flush();
            String result = out.toString();
        }

        String config = cmd.getOptionValue("config_file", "etcqqq/");

        System.out.println(config);

    }





}
