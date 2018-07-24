# clamda

A little Clojure library to help you reduce/transduce over Java Streams (including parallel ones).
Helpers for conveniently creating Lamdas from plain Clojure functions were inevitable.

## Where
TBC

## Why

This code started out as an attempt to write a parallel dictionary-attack tool (for great fun and NO profit) that could handle really
big files without memory linearly growing. Laziness, or even better, something reducible will do a great job at that,
but constrained me in one thread. I quickly realised that I could use Java Streams. In fact, when I found out that `Files.lines`
returned a Stream that could be turned to parallel rather easily - I was simply was ecstatic (turns out that's only true for JDK 9 and above).
Long story short, once the potential clicked, I simply wrote some convenience utilities, and some interesting things simply fell out.

All that said, the value of this library is most-likely NOT password-cracking, but rather Java interop (consuming Streams).

## How

### clamda.core

#### stream-into
A 'collecting' transducing context (along the lines of `clojure.core/into`), for Streams.
In some respects, I feel this is the centerpiece when it comes to Java interop.
No intermediate collections are involved, and in the case of a sequential Stream,
it can/will be done fully mutably. The same cannot be said for a parallel Stream, which
can do the 'outer' combining using transients (via `into`), but the 'inner' reductions
must not mutate their arguments, so `conj` *has to* be used (as opposed to `conj!`).


```clj
(use 'clamda.core)
(import '[java.util.stream LongStream]
        '[java.util Random])

(let [test-stream (LongStream/range 0 10)
         expected (range 0 10)]
      (= expected (stream-into [] test-stream))) ;; => true

(let [test-stream (-> (Random.) (.ints 10 1 50))]
      (every? odd? (stream-into [] (filter odd?) test-stream)) ;; => true

(let [test-stream (.parallel (LongStream/range 0 500))
          expected (range 1 501)]
      (= expected (stream-into [] (map inc) test-stream)))  ;; => true

```

##### Caveats
When dealing with a parallel Stream, you have to be extra careful with what you use as
init value, because that may (and probably will) be used in multiple reductions
(each in its own thread). An empty vector, or an empty anything will be fine, but depending
on your use-case, an non-empty target collection may not produce the results you expect.
Again, perhaps your logic does account for duplicates as the result of using the same
 (non-empty) init in multiple reductions, but maybe it doesn't. **Be aware and cautious**!

#### stream-some
An abortive transducing context for Streams.
For sequential Streams, rather similar to `.findFirst()` in terms of Streams,
or `clojure.core/some` in terms of lazy-seqs. For parallel Streams, more
like `.findAny()`, with the added bonus of being able to abort the search on the
'other' threads as soon as an answer is found on 'some' thread.

Even though the idea of `(first (filter some-pred some-seq))` is sort of an anti-pattern
 when dealing with lazy-seqs, it's actually a great patten when adapted for
  transducers/reducibles.

```clj
(use 'clamda.core)
(import '[java.util.stream LongStream]
        '[java.util Random])

(let [test-stream (LongStream/range 1 11)]
      ;; find first even number in the stream
      (= 2 (stream-some (filter even?) test-stream))) ;; => true

```

At this point implementing a parallel dictionary-attacker based on the (parallel) Stream
returned by `Files/lines` is trivial:

```clj
;; where `try-fn` is some undefined process accepting a candidate
;; and returning something truthy upon match
(stream-some (map try-fn) parallel-line-stream)
```

Mind you, 2GB is the maximum size of a file that the JVM will let you mmap.
In other words, you can safely forget about parallelism if you're dealing with files larger than 2GB (on the JVM).
If you're curious and would like to know more about that limitation see [here](https://bugs.java.com/bugdatabase/view_bug.do?bug_id=6347833). Essentially, it all comes down to int being 32-bit and being used as the type for array indexing.
The irony is that this decision was made around a time (mid 90s) when 64-bit CPUs were, somewhat, visible on the horizon,
let alone RAM sizes.


#### lines-reducible
An alternative to `clojure.core/line-seq`. Returns something reducible, rather than a lazy-seq.

If one is ok with a single-threaded dictionary-attack, it's worth considering the following.

```clj
(transduce     ;; no reason for Streams
  (map try-fn) ;; `try-fn` tries (somehow) each candidate
  rf-some
  (lines-reducible (io/reader "/path/to/dict.txt")))

```

I would be (pleasantly) surprised to find some other single-threaded approach
that would be faster than the above, as it basically boils down to a loop/recur
against a BufferedReader.
In fact, from what I can observe the above approach is showing very similar
timings as the sequential `Files/lines` Stream (via `.findFirst()`), something
which I find rather encouraging, and just goes to show that quite often
 there is very little to be gained dropping down to Java. You should
(always) benchmark for yourself though ;).

#### iterator-reducible
An alternative to `clojure.core/iterator-seq`. Returns something reducible, rather than a lazy-seq.


#### seq-stream
If you're writing Clojure, then you can probably ignore this function.
I don't see why you would want to convert a native Clojure data-structure
to a Java Stream. However, if you're writing Java, it is conceivable that
your code needs to consume a Clojure data-structure. In such (admittedly rare)
cases, it is only natural to want to express your code in terms of streams.

Here is roughly what you need to do in order to invoke `seq-stream` from Java.

```java
import clojure.java.api.Clojure;
import clojure.lang.IFn;
import clojure.lang.ISeq;

private static final IFn require = Clojure.var("clojure.core", "require");

public Stream seqStream (ISeq s, long splitSize, boolean parallel?){

    require.invoke(Clojure.read("clamda.core"));
    IFn toSeqStream = Clojure.var("clamda.core", "seq-stream");
    return toSeqStream.invoke(s, splitSize, parallel);
}
```

Parallelism is supported, but is enabled by default only for vectors.
If you wish to enable it for other types, or disable it for vectors,
use the 3-arg overload as shown above.
First argument is obviously the Seq in question, second is the partition
size for lazy-seqs (if parallelism is enabled), and third is whether we
want to allow for parallelism.

Some rudimentary benchmarking shows set/map being much slower (especially set)
than vector/lazy-seq, in both parallel and sequential execution.
That said, it needs to be noted that when considered in isolation,
there seems to be great benefit from going parallel (when the numbers grow).
Vectors are perform respectably in either context, especially when parallelised,
(that's why it's done by default for them).
Lazy-seqs can technically be parallelised but there is usually very little (to no) benefit
in terms of performance, as they cannot be split cheaply. In any case, it will always be
cheaper not to involve (yet) another abstraction (Stream) if you can afford it.
Again, **be aware and cautious!**

### clamda.jlamda
#### jlamda (multi-method dispatching on the first arg)
*You don't need to require this namespace if you're not adding implementations.
A wrapper function is provided in `clamda.core/jlamda` for convenience.*

The following implementations are provided:

* `[:predicate f]` where `f` is a fn of 1 arg
* `[:function f]` where `f` is a fn of 1 arg
* `[:bi-function f]` where `f` is a fn of 2 args
* `[:consumer f]` where `f` is a fn of 1 arg
* `[:long-consumer f]` where `f` is a fn of 1 arg
* `[:int-consumer f]` where `f` is a fn of 1 arg
* `[:double-consumer f]` where `f` is a fn of 1 arg
* `[:bi-consumer f]` where `f` is a fn of 2 args
* `[:supplier f]` where `f` is a fn of 0 args
* `[:long-supplier f]` where `f` is a fn of 0 args
* `[:int-supplier f]` where `f` is a fn of 0 args
* `[:double-supplier f]` where `f` is a fn of 0 args
* `[:unary f]` where `f` is a fn of 1 arg
* `[:long-unary f]` where `f` is a fn of 1 arg
* `[:int-unary f]` where `f` is a fn of 1 arg
* `[:double-unary f]` where `f` is a fn of 1 arg
* `[:binary f]` where `f` is a fn of 2 args

Primitive casts are implicitly added where needed.

## WARNING
### Be wary of parallel streams
#### init values
As a general rule of thumb, be extra careful with what you hand off as the init value to a parallel reduction.
There is no one-size-fits-all recipe here - it all depends on the function that's reducing. In the case of
`stream-into`, the reducing-fn is (unsurprisingly) `conj` or `conj!` (when possible). So, simply put, two threads receiving
the same non-empty init value, will produce duplicates (unless init is a set of course).
The duplicates will be N copies of the initial init elements, where N will end up being the number of threads
that used that init for their internal reduction.

#### collecting mutably
Collecting mutably (via transients) is no problem for sequential streams.
Parallel reductions can NOT be done (fully) mutably - only their (eventual)
combining, and that's exactly what `stream-into` does. It uses `conj` for
the 'inner' reductions, and `conj!` (via `into`) for combining their results.


## TL;DR
If you want to (idiomatically) consume Java streams in Clojure, or Clojure seqs in Java,
then you've come to the right place. For Clojure users, see `stream-reducible`, `stream-into`,
`stream-some`, and the `jlamda` constructor-fn. For Java users, check-out `seq-stream`
(see the 'How' section for instructions). Do NOT expect that processing a SeqStream with Lamdas
will give you anywhere near the performance of a native (mutable) Stream
(or even that of processing the same seq directly in Clojure),
unless perhaps in the case of vectors (that happen to parallelize nicely too).

All the aforementioned functions exist in the `clamda.core` namespace.


## License

Copyright © 2018 Dimitrios Piliouras

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
