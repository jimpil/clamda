(defproject clamda "0.1.1-SNAPSHOT"
  :description "FIXME: write description"
  :url "https://github.com/jimpil/clamda"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.9.0"]]
  ;; exact match of the test dictionary
  :jar-exclusions [#"ddict\.txt"])
