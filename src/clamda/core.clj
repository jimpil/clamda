(ns clamda.core
  (:require [clamda.redux :refer [abortive-stream vrest mrest rrest]]
            [clamda.jlamda :as jl])
  (:import [java.util.stream Stream StreamSupport]
           [clojure.lang IReduceInit]
           (java.io BufferedReader)
           (java.util Iterator)
           (clamda.redux SeqSpliterator)))

(defn- accu*
  "A little helper for creating accumulators."
  [f done acc v]
  (let [ret (f acc v)]
    (if (reduced? ret)
      (do (done) @ret)
      ret)))

(defn- throw-combine-not-provided!
  [_ _]
  (throw
    (IllegalStateException.
      "combine-fn NOT provided!")))

(defn rf-some
  "Reducing cousin of `clojure.core/some`."
  ([] nil)
  ([x] x)
  ([_ x]
   (when x
     (ensure-reduced x))))

;;=================================

(defn stream-reducible
  "Turns a Stream into something reducible,
   and optionally short-circuiting (via the 3-arg overload)."
  ([s]
   (stream-reducible s throw-combine-not-provided!))
  ([s combinef]
   (stream-reducible s combinef false))
  ([^Stream s combinef abortive?]
   (reify IReduceInit
     (reduce [_ f init]
       (let [[done? done!] (when abortive?
                             (let [dp (promise)]
                               [(partial realized? dp)
                                (partial deliver dp true)]))
             done? (or done? (constantly false))
             done! (or done! (constantly nil))
             bi-function (jl/jlamda :bi-function (partial accu* f done!)) ;; accumulator
             binary-op   (jl/jlamda :binary combinef)]       ;; combiner
         (with-open [estream (abortive-stream s done?)]
           (.reduce estream init bi-function binary-op)))))))

(defn stream-into
  "A 'collecting' transducing context (like `clojure.core/into`), for Java Streams.
   Useful for pouring streams into clojure data-structures
   without involving an intermediate Collection, with the added bonus
   of being able apply a transducer along the way.
   Parallel streams are supported, but there are two caveats. First of all <to> MUST be
   either empty, or something that can handle duplicates (e.g a set), because it will become
   the <init> for more than one reductions. Be AWARE & CAUTIOUS!
   Secondly, `Stream.reduce()`requires that the reduction does not mutate the values
   received as arguments to combine. Therefore, `conj` has to be the reducing fn per
   parallel reduction, leaving `conj!` (via `into`) for the 'outer' combining.
   For serial streams we can go fully mutably (much like `.collect()` does)."
  ([to ^Stream stream]
   (if (.isParallel stream)
     (reduce conj to (stream-reducible stream into))
     (into to (stream-reducible stream))))
  ([to xform ^Stream stream]
   (if (.isParallel stream)
     ;; Cannot use `into` on a parallel stream because it may use transients.
     ;; That goes against the requirement that the reduction
     ;; does not mutate the values received as arguments to combine.
     (transduce xform conj to (stream-reducible stream into))
     ;; for a serial stream we're golden - just delegate to `into`
     (into to xform (stream-reducible stream)))))


(defn stream-some
  "A short-circuiting transducing context for Java streams (parallel or not).
   For sequential Streams, rather similar to `.findFirst()` in terms of Streams,
   or `clojure.core/some` in terms of lazy-seqs. For parallel Streams, more
   like `.findAny()`, with the added bonus of aborting the search on the
   'other' threads as soon as an answer is found on 'some' thread."
  ([xform stream]
   (stream-some identity xform stream))
  ([combine-f xform ^Stream stream]
   (let [combine (if (.isParallel stream)
                   (some-fn combine-f)
                   throw-combine-not-provided!)]
     (transduce xform rf-some (stream-reducible stream combine true)))))

(defn lines-reducible
  "Similar to `clojure.core/line-seq`, but
   returns something reducible instead of a lazy-seq."
  [^BufferedReader r]
  (reify IReduceInit
    (reduce [_ f init]
      (with-open [rdr r]
        (loop [state init]
          (if (reduced? state)
            @state
            (if-let [line (.readLine rdr)]
              (recur (f state line))
              state)))))))


(defn iterator-reducible
  "Similar to `clojure.core/iterator-seq`,
   but returns something reducible instead of a lazy-seq."
  [^Iterator iter]
  (reify IReduceInit
    (reduce [_ f init]
      (loop [it iter
             state init]
        (if (reduced? state)
          @state
          (if (.hasNext it)
            (recur it (f state (.next it)))
            state))))))

(defn seq-stream
  "Returns a Stream around a Clojure seq.
   The usefulness of this constructor-fn is minimal (to none) for Clojure users.
   If you're writing Java, it is conceivable that you may want/have to consume a Clojure
   data-structure, and in such cases it is only natural to want to write your code
   in terms of streams.
   WARNING: Only vectors are cheaply splittable, so in general avoid forcefully parallelizing,
   unless you know what you're doing."
  ([s]
   (seq-stream s 1024))
  ([s lazy-split]
    ;; allow parallelism by default only on vectors
    ;; because they can be split cheaply
   (seq-stream s lazy-split (vector? s)))
  (^Stream [s lazy-split parallel?]
   (let [rest-fn (cond ;; 2-args so we can call it without caring
                   (vector? s) vrest
                   (set? s)    disj
                   (map? s)    mrest
                   :else       rrest)]
     (StreamSupport/stream
       (SeqSpliterator. s rest-fn (or lazy-split 1024))
       parallel?))))

(defn jlamda
  "Convenience wrapper-fn around `clamda.jlamda/jlamda`.
   Type-hinting at the call site may be required (to avoid reflection)."
  [t f]
  (jl/jlamda t f))