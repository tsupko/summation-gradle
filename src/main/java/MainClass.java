import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Вариант 3 / Variant 3
 *
 * Необходимо разработать программу, которая получает на вход список ресурсов,
 * содержащих набор чисел, и считает сумму всех положительных четных.
 * Каждый ресурс должен быть обработан в отдельном потоке,
 * набор должен содержать лишь числа, унарный оператор "-" и пробелы.
 * Общая сумма должна отображаться на экране и изменяться в режиме реального времени.
 * Все ошибки должны быть корректно обработаны, все API покрыто модульными тестами.
 *
 * The task is to develop a program which receives a list of resources
 * containing a set of numbers, and computes the sum of all positive even ones.
 * Every resource has to be handled in a separate thread,
 * the set must contain just the numbers, unary operator "-" and spaces.
 * The total sum should be displayed at the screen and change in real time.
 * All the errors have to be correctly handled, all APIs should be covered with module tests.
 *
 * @author Alexander Tsupko (tsupko.alexander@yandex.ru)
 *         Copyright (c) 2016. All rights reserved.
 */
public class MainClass {
    private static final Logger LOGGER = LoggerFactory.getLogger(MainClass.class); // logging instance constant
    private static final File PATH = new File(String.format("%s/src/test/resources", System.getProperty("user.dir")));
    // location of resources (for testing, change from ".../main/..." to ".../test/...", and vice versa)
    private static final int DEFAULT_NUMBER = Integer.MAX_VALUE >> 28; // default number of resources and integers
    // per resource (current value equals 7)
    private static final long TIMEOUT = 1L; // timeout to wait for completing a particular task
    private static final TimeUnit TIME_UNIT = TimeUnit.MINUTES; // time unit of the timeout
    private static final long SLEEP_DURATION = 4_000L; // duration for a thread to sleep before execution (in ms)

    private static int number; // number of resources
    static {
        // suppressed warning says there might not be any required resources in the path giving a NullPointerException
        @SuppressWarnings("ConstantConditions") int length = PATH.list(((dir, name) -> name.endsWith(".txt"))).length;
        if (length < DEFAULT_NUMBER) {
            number = DEFAULT_NUMBER; // if there are no resources in the path, generate a default number of ones
            LOGGER.debug("There are not enough resources to process. Generating {} resources by default...", number);
            GenerateResources.main(String.valueOf(number));
        } else {
            number = length;
            LOGGER.info("There are enough resources to process. The exact number is {}", number);
        }
    }
    private static List<Future<AtomicLong>> list = new ArrayList<>(); // list of tasks
    private static Stream.Builder<Long> streamBuilder = Stream.builder(); // stream for intermediate accumulates
    private static AtomicLong total = new AtomicLong(0L); // total result

    /**
     * Getter method for the result of computation, converted to long primitive type.
     *
     * @return the result of computation
     */
    public static long getTotal() {
        return total.get();
    }

    /**
     * The main method creates executor service of a fixed thread pool with number of threads equal to that of resources.
     * Using the new Java 8 Stream API allows quite efficiently articulate the solution to the problem at hand.
     * To make the access to the running total synchronized, we use {@code AtomicLong} variable.
     *
     * @param args for the moment being, the program does not require any command-line arguments
     */
    public static void main(String... args) {
        go();
    }

    /**
     * The same as the main method. It serves as a starting point of the whole program.
     */
    private static void go() {
        ExecutorService executorService = null;
        try {
            executorService = Executors.newFixedThreadPool(number); // create a thread pool for a number of resources
            LOGGER.debug("New fixed thread pool of size {} created", number);
            submitTasksToExecutorService(executorService, SLEEP_DURATION); // submit the tasks to the executor service
            streamBuilder.build().forEachOrdered(System.out::println); // build a stream and print intermediate results
            LOGGER.info("New {@code Stream<Long>} built and printed out successfully");
        } finally {
            if (executorService != null) {
                executorService.shutdown(); // shutdown the executor service
                LOGGER.debug("Executor service shutdown");
            }
        }
        System.out.printf("TOTAL: %d", getTotal()); // print out the total result
    }

    /**
     * The method submits the tasks in the loop to the executor service and waits for them to complete.
     * It uses Stream API to run the pipeline processing in parallel and to obtain intermediate results.
     * Additionally, it puts the tasks to the {@code List<Future<?>>} to enable later tracking of them.
     *
     * @param executorService the service reference to submit the tasks to and to assign to the {@code Future<?>}
     * @param sleepDuration the quantity to make a thread sleep for a random amount of time before starting execution
     */
    private static void submitTasksToExecutorService(ExecutorService executorService, long sleepDuration) {
        for (int i = 1; i <= number; i++) {
            int finalI = i; // using the changing loop variable in a lambda requires it to be effectively final
            Future<AtomicLong> submit = executorService.submit(() -> {
                try (BufferedReader bufferedReader = new BufferedReader(
                        new FileReader(String.format("%s/resource%d.txt", PATH, finalI)));
                     Stream<String> streamString = bufferedReader.lines()) {
                    Thread.sleep((long)(Math.random() * sleepDuration)); // sleep for up to {sleepDuration} seconds
                    streamBuilder.accept( // put intermediate total to the stream builder
                            total.getAndAdd( // atomically add to the current total
                                    streamString // take the stream of strings created from the resource
                                    .parallel() // use parallel processing of the stream of strings
                                    .mapToLong(Long::parseLong) // parse strings to long primitive type
                                    .filter(e -> e > 0 && e % 2 == 0) // filter the required values
                                    .reduce((left, right) -> left + right) // add filtered values together
                                    .orElseGet(() -> 0) // get as a long primitive or zero if not present
                            )
                    );
                    LOGGER.info("New {@code Stream<String>} for the resource #{} created and processed", finalI);
                    return total;
                }
            });
            LOGGER.info("New task {} submitted", submit);
            list.add(submit); // adding tasks to the list enables waiting for their completion
            LOGGER.info("The task {} added to the list of tasks", submit);
        }
        waitForAllTasksToComplete(TIMEOUT, TIME_UNIT); // if a task takes too long, its results will be discarded
    }

    /**
     * The method waits for all submitted tasks to complete for the specified amount of time expressed in time units.
     *
     * @param timeout the amount of time to wait
     * @param timeUnit the unit of time
     */
    private static void waitForAllTasksToComplete(long timeout, TimeUnit timeUnit) {
        for (Future<AtomicLong> submit : list) {
            try {
                if (!submit.isDone()) { // if the current task is not yet complete
                    submit.get(timeout, timeUnit); // wait for it the specified time to complete
                    LOGGER.info("Waiting for the task {} to complete", submit);
                }
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                LOGGER.error("Exception caught: {}", e.getMessage());
            }
        }
    }

    /**
     * The class serves for generating the specified number of resources (currently, text files)
     * containing the specified (currently, the same as number of resources) number of integers per resource,
     * randomly distributed in the range from {@code Integer.MIN_VALUE + 2} to {@code Integer.MAX_VALUE - 1}.
     *
     * @author Alexander Tsupko (tsupko.alexander@yandex.ru)
     *         Copyright (c) 2016. All rights reserved.
     */
    private static class GenerateResources {
        private static int number = 0; // current number of resources
        private static int size = DEFAULT_NUMBER; // number of integers per resource (currently 7)

        private static final Random RANDOM = new Random(); // variable for generating random values

        /**
         * The main method either takes a single parameter which signifies the desirable numbers of resources
         * and integers per resource, or takes none, in which case the default value mentioned above is used.
         *
         * @param args if present, specifies the number of resources and
         *             the number of integers generated per resource;
         *             otherwise, default value is used.
         *             The recommended values range between 1 and about 31.
         */
        public static void main(String... args) {
            if (args.length == 1) {
                size = Integer.parseInt(args[0]);
                LOGGER.info("Variable {@code size} initialized from a command-line argument to {}", size);
            } else if (args.length > 1) {
                LOGGER.debug("More than one command-line argument passed in. Needed one or none.");
                return;
            }
            generateResources(size);
        }

        /**
         * The method generates a {@code number} of resources (e.g., text files) containing the randomly distributed
         * integers from almost all possible {@code Integer} values. The number of integers per resource corresponds
         * to that of the resources, and may be specified via the command-line argument or taken the default value.
         *
         * @param size the number of resources and integers per resource to generate
         */
        private static void generateResources(int size) {
            while (size > number++) {
                try (PrintWriter printWriter = new PrintWriter(
                        new FileOutputStream(PATH + "/resource" + number + ".txt"))) {
                    for (int i = 0; i < size; i++) {
                        printWriter.println(
                                (int)(Math.random() * Integer.MAX_VALUE) * (RANDOM.nextBoolean() ? 1 : -1)
                        );
                    }
                    LOGGER.info("{} resources generated successfully", number);
                } catch (IOException e) {
                    LOGGER.error("Exception caught: {}", e.getMessage());
                }
            }
        }
    }
}
