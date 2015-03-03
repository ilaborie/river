package org.ilaborie.jdk8;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;

/**  */
public class Partition<T> implements Collector<T, List<List<T>>, List<List<T>>> {

    private final int size;

    public Partition(int size) {
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
                List<List<T>> result = new ArrayList<>(list1.size() + list2.size());

                // Append completed lists
                if (list1.size() > 1) {
                    result.addAll(list1.subList(0, list1.size() - 1));
                }
                if (list2.size() > 1) {
                    result.addAll(list2.subList(0, list2.size() - 1));
                }

                // Handle last lists
                List<T> last1 = list1.get(list1.size() - 1);
                List<T> last2 = list2.get(list2.size() - 1);


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
        return Collections.singleton(Collector.Characteristics.IDENTITY_FINISH);
    }
}
