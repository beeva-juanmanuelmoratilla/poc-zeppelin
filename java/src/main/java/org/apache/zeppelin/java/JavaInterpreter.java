package org.apache.zeppelin.java;


import org.apache.commons.exec.*;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.zeppelin.interpreter.Interpreter;
import org.apache.zeppelin.interpreter.InterpreterContext;
import org.apache.zeppelin.interpreter.InterpreterResult;
import org.apache.zeppelin.interpreter.thrift.InterpreterCompletion;
import org.apache.zeppelin.scheduler.Job;
import org.apache.zeppelin.scheduler.Scheduler;
import org.apache.zeppelin.scheduler.SchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;


public class JavaInterpreter extends Interpreter {

    Logger logger = LoggerFactory.getLogger(JavaInterpreter.class);
    private static final String EXECUTOR_KEY = "executor";
    private static final boolean isWindows = System
            .getProperty("os.name")
            .startsWith("Windows");
    final String shell = isWindows ? "cmd /c" : "bash -c";

    private final String JAVA_FILES_FOLDER = "java.code.folder";
    private final String JAVA_LIBRARY_FOLDER = "java.libraries.folder";

    String javaFilesFolder;
    String librariesFolder;

    public JavaInterpreter(Properties property) {
        super(property);
    }

    @Override
    public void open() {
        javaFilesFolder = String.valueOf(getProperty(JAVA_FILES_FOLDER));
        librariesFolder = String.valueOf(getProperty(JAVA_LIBRARY_FOLDER));
        logger.info("Code folder:", JAVA_FILES_FOLDER);
        logger.info("Library folder:", librariesFolder);
    }

    @Override
    public void close() {
    }

    private String getClassName(String cmd){
        String[] lines = StringUtils.split(cmd, "\n");

        String[] words;
        int i=0;
        boolean found = false;
        String className = "NO_CLASS_NAME";

        while (i<=lines.length && !found){
            words = StringUtils.split(lines[i], " ");
            if (StringUtils.equals(words[0], "public")){
                found = true;
                className = words[2];
            }
            i++;
        }
        return className;
    }


    @Override
    public InterpreterResult interpret(String cmd, InterpreterContext contextInterpreter) {
        logger.debug("Running Java command '" + cmd + "'");

        String fileRoute = javaFilesFolder+"/"+getClassName(cmd);

        try {
            FileUtils.writeStringToFile(new File(fileRoute+".java"), cmd);
        } catch (IOException e) {
            e.printStackTrace();
        }

        CommandLine cmdLine = CommandLine.parse(shell);
        // the Windows CMD shell doesn't handle multiline statements,
        // they need to be delimited by '&&' instead
        if (isWindows) {
            String[] lines = StringUtils.split(cmd, "\n");
            cmd = StringUtils.join(lines, " && ");
        }
        cmdLine.addArgument(
                "rm -f "+ fileRoute+".class"+"\n"+
                "javac"+" -cp \".:"+librariesFolder+"/*\""+" "+fileRoute+".java"+"\n"+
                "export CLASSPATH="+javaFilesFolder+"\n"+
                "java "+getClassName(cmd), false);
        DefaultExecutor executor = new DefaultExecutor();
        executor.setStreamHandler(new PumpStreamHandler(contextInterpreter.out,
                contextInterpreter.out));
        executor.setWatchdog(new ExecuteWatchdog(5000));

        Job runningJob = getRunningJob(contextInterpreter.getParagraphId());
        Map<String, Object> info = runningJob.info();
        info.put(EXECUTOR_KEY, executor);
        try {
            int exitVal = executor.execute(cmdLine);
            logger.info("Paragraph " + contextInterpreter.getParagraphId()
                    + "return with exit value: " + exitVal);
            return new InterpreterResult(InterpreterResult.Code.SUCCESS, null);
        } catch (ExecuteException e) {
            int exitValue = e.getExitValue();
            logger.error("Can not run " + cmd, e);
            InterpreterResult.Code code = InterpreterResult.Code.ERROR;
            String msg = null;
            try {
                contextInterpreter.out.flush();
                msg = new String(contextInterpreter.out.toByteArray());
            } catch (IOException e1) {
                logger.error(e1.getMessage());
                msg = e1.getMessage();
            }
            if (exitValue == 143) {
                code = InterpreterResult.Code.INCOMPLETE;
                msg = msg + "Paragraph received a SIGTERM.\n";
                logger.info("The paragraph " + contextInterpreter.getParagraphId()
                        + " stopped executing: " + msg);
            }
            msg += "ExitValue: " + exitValue;
            return new InterpreterResult(code, msg);
        } catch (IOException e) {
            logger.error("Can not run " + cmd, e);
            return new InterpreterResult(InterpreterResult.Code.ERROR, e.getMessage());
        }
    }

    private Job getRunningJob(String paragraphId) {
        Job foundJob = null;
        Collection<Job> jobsRunning = getScheduler().getJobsRunning();
        for (Job job : jobsRunning) {
            if (job.getId().equals(paragraphId)) {
                foundJob = job;
            }
        }
        return foundJob;
    }

    @Override
    public void cancel(InterpreterContext context) {
        Job runningJob = getRunningJob(context.getParagraphId());
        if (runningJob != null) {
            Map<String, Object> info = runningJob.info();
            Object object = info.get(EXECUTOR_KEY);
            if (object != null) {
                Executor executor = (Executor) object;
                ExecuteWatchdog watchdog = executor.getWatchdog();
                watchdog.destroyProcess();
            }
        }
    }

    @Override
    public FormType getFormType() {
        return FormType.SIMPLE;
    }

    @Override
    public int getProgress(InterpreterContext context) {
        return 0;
    }

    @Override
    public Scheduler getScheduler() {
        return SchedulerFactory.singleton().createOrGetParallelScheduler(
                JavaInterpreter.class.getName() + this.hashCode(), 10);
    }

    @Override
    public List<InterpreterCompletion> completion(String buf, int cursor) {
        return null;
    }

}
