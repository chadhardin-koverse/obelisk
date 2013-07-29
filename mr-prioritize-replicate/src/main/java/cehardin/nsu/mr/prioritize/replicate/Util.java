package cehardin.nsu.mr.prioritize.replicate;

import com.google.common.collect.Iterables;
import static com.google.common.collect.Iterables.size;
import static com.google.common.collect.Iterables.get;
import static com.google.common.collect.Iterables.contains;
import static com.google.common.collect.Iterables.concat;
import com.google.common.collect.Lists;
import static com.google.common.collect.Lists.newArrayList;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Random;

/**
 *
 * @author cehar_000
 */
public class Util {
//    public static <T> T pickRandom(final Random random, final Iterable<T> items) {
//        return pickRandom(random, items, new ArrayList<T>(0));
//    }
    
     public static <T> T pickRandom(final Random random, final Iterable<T> items, final T... not) {
        return pickRandom(random, items, newArrayList(not));
    }
    
    public static <T> T pickRandom(final Random random, final Iterable<T> items, final Iterable<T> not) {
        T chosen = null;
        
        do {
            final int index = random.nextInt(size(items));
            chosen = get(items, index);
        } while(contains(not, chosen));
        
        return chosen;
    }
    
    public static <T> Iterable<T> pickRandomPercentage(final Random random, final Collection<T> items, final double percentage) {
        return pickRandomPercentage(random, items, new ArrayList<T>(), percentage);
    }
    
    public static <T> Iterable<T> pickRandomPercentage(final Random random, final Collection<T> items, final Iterable<T> not, final double percentage) {
        return pickRandom(random, items, not, (int)(items.size() * percentage));
    }
    
    public static <T> Iterable<T> pickRandom(final Random random, final Iterable<T> items, final Iterable<T> not, final int amount) {
        final Collection<T> chosen = newArrayList();
        
        while(chosen.size() < amount) {
            chosen.add(pickRandom(random, items, concat(chosen, not)));
        }
        
        return chosen;
    }
}
