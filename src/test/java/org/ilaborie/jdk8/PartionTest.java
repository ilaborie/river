package org.ilaborie.jdk8;

import com.google.common.base.Stopwatch;
import org.junit.Test;

import java.util.List;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;

/**  */
public class PartionTest {

    private List<List<Integer>> partition(int partitionSize, int count) {
        Stopwatch stopwatch = Stopwatch.createStarted();
        try {
            return IntStream.range(0, count)
                    .mapToObj(Integer::valueOf)
                    .collect(new Partition<>(partitionSize));
        } finally {
            System.out.printf("%d took: %s\n", count, stopwatch.stop());
        }
    }

    private List<List<Integer>> partitionParallel(int partitionSize, int count) {
        Stopwatch stopwatch = Stopwatch.createStarted();
        try {
            return IntStream.range(0, count)
                    .parallel()
                    .mapToObj(Integer::valueOf)
                    .collect(new Partition<>(partitionSize));
        } finally {
            System.out.printf("%d // took: %s\n", count, stopwatch.stop());
        }
    }


    @Test
    public void testPartitionBasic() throws Exception {
        int partitionSize = 50;
        List<List<Integer>> partition = partition(partitionSize, 106);

        assertEquals(3, partition.size());

        List<Integer> first = partition.get(0);
        assertEquals(partitionSize, first.size());

        List<Integer> second = partition.get(1);
        assertEquals(partitionSize, second.size());

        List<Integer> third = partition.get(2);
        assertEquals(6, third.size());
    }

    @Test
    public void testPartitionBig() throws Exception {
        int count = 19_320;
        int partitionSize = 50;
        List<List<Integer>> partition = partition(partitionSize, count);

        int number = (int) Math.ceil(((double) count) / partitionSize);
        assertEquals(number, partition.size());

        int lastSize = count % partitionSize;
        List<Integer> last = partition.get(partition.size() - 1);
        assertEquals(lastSize, last.size());
    }


    @Test
    public void testPartitionHuge() throws Exception {
        int count = 5_623_756;
        int partitionSize = 50;
        List<List<Integer>> partition = partition(partitionSize, count);

        int number = (int) Math.ceil(((double) count) / partitionSize);
        assertEquals(number, partition.size());

        int lastSize = count % partitionSize;
        List<Integer> last = partition.get(partition.size() - 1);
        assertEquals(lastSize, last.size());
    }

    @Test
    public void testPartitionBasicParallel() throws Exception {
        int partitionSize = 50;
        List<List<Integer>> partition = partitionParallel(partitionSize, 106);

        assertEquals(3, partition.size());

        List<Integer> first = partition.get(0);
        assertEquals(partitionSize, first.size());

        List<Integer> second = partition.get(1);
        assertEquals(partitionSize, second.size());

        List<Integer> third = partition.get(2);
        assertEquals(6, third.size());
    }

    @Test
    public void testPartitionBigParallel() throws Exception {
        int count = 19_320;
        int partitionSize = 50;
        List<List<Integer>> partition = partitionParallel(partitionSize, count);

        int number = (int) Math.ceil(((double) count) / partitionSize);
        assertEquals(number, partition.size());

        int lastSize = count % partitionSize;
        List<Integer> last = partition.get(partition.size() - 1);
        assertEquals(lastSize, last.size());
    }


    @Test
    public void testPartitionHugeParallel() throws Exception {
        int count = 5_623_756;
        int partitionSize = 50;
        List<List<Integer>> partition = partitionParallel(partitionSize, count);

        int number = (int) Math.ceil(((double) count) / partitionSize);
        assertEquals(number, partition.size());

        int lastSize = count % partitionSize;
        List<Integer> last = partition.get(partition.size() - 1);
        assertEquals(lastSize, last.size());
    }


}
