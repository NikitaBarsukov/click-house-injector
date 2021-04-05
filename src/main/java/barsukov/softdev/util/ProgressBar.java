package barsukov.softdev.util;

public class ProgressBar {

    private static final int barsNum = 10;

    public static void updateProgressBar(Long input, Long total){
        var currentPercent = calculatePerc(input, total);
        var n = calculateN(currentPercent);
        System.out.print(
                drawSymbols(n) + " ".repeat(barsNum - n) + "|     " +
                        currentPercent + "%     " + input + "/" + total
                        + "b\r"
        );
    };

    private static String drawSymbols(int times) {
        return "\u2588".repeat(times);
    }

    private static int calculatePerc(Long input, Long total){
        return (int) ((100 * input) / total);
    }

    private static int calculateN(int percent){
        return (percent/barsNum);
    }
}