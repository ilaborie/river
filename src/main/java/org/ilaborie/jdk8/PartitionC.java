package org.ilaborie.jdk8;

import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;

import static java.util.Collections.unmodifiableSet;

/**  */
public class PartitionC<T> implements Collector<T, List<List<T>>, List<List<T>>> {

    private final int size;

    public PartitionC(int size) {
        super();
        this.size = size;
    }

    @Override
    public Supplier<List<List<T>>> supplier() {
        return ArrayList::new;
    }

    @Override
    public BiConsumer<List<List<T>>, T> accumulator() {
        return (list, elt) -> {
            assert list != null;
            if (list.isEmpty()) {
                List<T> newList = new ArrayList<>(size);
                newList.add(elt);
                list.add(newList);
            } else {
                List<T> eltList = list.get(list.size() - 1);
                if (eltList.size() >= size) {
                    List<T> newList = new ArrayList<>(size);
                    newList.add(elt);
                    list.add(newList);
                } else {
                    eltList.add(elt);
                }
            }
        };
    }

    @Override
    public BinaryOperator<List<List<T>>> combiner() {
        return (list1, list2) -> {
            assert list1 != null;
            assert list2 != null;
            if (list1.isEmpty()) {
                return list2;
            } else if (list2.isEmpty()) {
                return list1;
            } else {
                List<T> last1 = list1.remove(list1.size() - 1);
                List<T> last2 = list2.remove(list2.size() - 1);

                // Append completed lists
                List<List<T>> result = new ArrayList<>(list1.size() + list2.size());
                result.addAll(list1);
                result.addAll(list2);

                // Handle last lists
                int size2 = last2.size();

                int missing = size - last1.size();
                if (missing == 0) {
                    // last1 full
                    result.add(last1);
                    result.add(last2);
                } else if (missing >= size2) {
                    // last2 go inside last1
                    last1.addAll(last2);
                    result.add(last1);
                } else {
                    // split last2 to fill last1 and append new list
                    last1.addAll(last2.subList(0, missing));
                    result.add(last1);
                    result.add(last2.subList(missing, size2));
                }
                return result;
            }
        };
    }

    @Override
    public Function<List<List<T>>, List<List<T>>> finisher() {
        return result -> result;
    }

    @Override
    public Set<Characteristics> characteristics() {
        return unmodifiableSet(EnumSet.of(Characteristics.UNORDERED, Characteristics.IDENTITY_FINISH));
    }
}
