(ns clamda.core-test
  (:require [clojure.test :refer :all]
            [clamda.core :refer :all])
  (:import (java.util.stream LongStream Collectors)
           (java.util Random ArrayList Arrays)))

(defn hard-worker
  [f ms]
  (fn [& args]
    (Thread/sleep ms)
    (apply f args)))

(deftest stream-into-tests

  (testing "plain `stream-into`"
    (let [test-stream (LongStream/range 0 10)
          expected (range 0 10)]
      (is (= expected
             (stream-into [] test-stream)))))

  (testing "`stream-into` with transducer"
    (let [test-stream (.ints (Random.) 50 1 100)]
      (is (every? odd? (stream-into [] (filter odd?) test-stream)))))

  (testing "`stream-into` with transducer and non-empty <init>"
    (let [test-stream (LongStream/range 0 10)
          xform (filter even?)
          expected (into [:a] (filter even? (range 0 10)))]
      (is (= expected
             (stream-into [:a] xform test-stream)))))

  (testing "`stream-into` with transducer against parallel stream"
    (let [test-stream (.parallel (LongStream/range 0 500))
          xform (map inc)
          expected (range 1 501)]
      (is (= expected
             (stream-into [] xform test-stream)))))

  (testing "breaking `stream-into` when the stream is parallel and <init> is not empty"
    (let [test-stream (.parallel (LongStream/range 0 500))
          expected (into [:a] (range 1 501))]
      (is (not= expected ;; there are duplicate :a because `[:a]` became the <init> in more than one thread
                (stream-into [:a] test-stream)))))

  )


(deftest stream-some-tests

  (testing "find first even number in the stream"
    (let [test-stream (LongStream/range 1 11)]
      (is (= 2 (stream-some (filter even?) test-stream)))))

  (testing "find the first even number, and add 10 to it before returning it"
    (let [test-stream (LongStream/range 1 10)]
      (is (= 12 (stream-some (keep #(when (even? %)
                                      (+ 10 %)))
                             test-stream)))))

  (testing "incrementing an odd number will always return an even number"
    (let [test-stream (.ints (Random.) 50 1 100)]
      (is (even? (stream-some (comp (filter odd?)
                                    (map inc))
                              test-stream)))))
  )



(defn- do-seq-stream-test
  ([s]
   (do-seq-stream-test s [odd? inc]))
  ([s [f1 f2]]
   (let [test-seq s
         test-seq-stream (seq-stream test-seq 10000 true)
         pred-lamda (jlamda :predicate f1)
         fn-lamda (jlamda :function f2)
         expected  (time
                     (->> test-seq
                          (filter f1)
                          (mapv f2)))
         ^ArrayList actual (time
                             (-> test-seq-stream
                                 (.filter pred-lamda)
                                 (.map    fn-lamda)
                                 (.collect (Collectors/toList))))]
     (is (true?
           (Arrays/equals (into-array expected)
                          (.toArray actual)))))))


(deftest seq-stream-tests

  (let [r (map inc (range 100000))
        non-map-fns [odd? inc]
        map-fns (map #(comp % key) non-map-fns)]

    (testing "a plain lazy-seq"
      ;; "Elapsed time: 77.792888 msecs"
      (do-seq-stream-test r non-map-fns))

    (testing "a vector"
      ;; "Elapsed time: 59.028358 msecs"
      (do-seq-stream-test (into [] r) non-map-fns))

    (testing "a set"
      ;; "Elapsed time: 1425.237297 msecs" - 83937.941167 when sequentially!
      (do-seq-stream-test (into #{} r) non-map-fns))

    (testing "a map"
      ;; "Elapsed time: 1437.287605 msecs" - 26028.905776 when sequentially!
      (do-seq-stream-test (zipmap r (repeat :a)) map-fns))

    )

  )