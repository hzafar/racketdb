#lang racket

(require
  "reql-term.rkt"
  (for-syntax "reql-term.rkt") ; there must be a better way
  (rename-in "proto/ql2.rkt"
    [version-dummy:version->integer v->int]
    [term:term-type->integer term->int])
  (rename-in racket/base
    [not not-]
    [floor floor-]
    [round round-]
    [append append-] 
    [values values-]
    [map map-]
    [filter filter-]
    [or or-]
    [and and-]
    [for-each for-each-])
  json
  (for-syntax racket/syntax))

(provide
  build-query r:lambda
  connect close-connection datum array object db db-create db-drop db-list table get get-all eq ne
  lt le gt ge add sub mul div mod ceil prepend difference set-insert set-intersection set-union
  set-difference slice skip limit offsets-of contains get-field keys has-fields with-fields pluck
  without merge between reduce fold concat-map order-by distinct count is-empty union nth bracket
  inner-join outer-join eq-join zip range insert-at delete-at change-at splice-at coerce-to type-of
  update delete replace insert table-create table-drop table-list config wait reconfigure rebalance
  sync grant index-create index-drop index-list index-status index-wait index-rename set-write-hook
  get-write-hook funcall branch run
  (rename-out
    [not r:not]
    [floor r:floor] 
    [round r:round]
    [append r:append] 
    [values r:values]
    [map r:map]
    [filter r:filter]
    [or r:or]
    [and r:and]
    [for-each r:for-each]
    [build-query/partial partial])) 

;; *************************************************************************************************
;; *************************************************************************************************

;; Read a null-terminated string from the input stream in.
(define (read-null-term-str in)
  (define (iter acc)
    (let ([c (read-char in)])
      (if (char=? c #\nul) (list->string (reverse acc)) (iter (cons c acc)))))
  (iter '()))

;; connect represents a connection to (optionally specified) host and port, with dbname used as the
;; default database. Specifying a default database at connection time means all queries run on that
;; connection may omit the database.
 
;; See the RethinkDB driver guide for details on what this method is doing.
;; TODO: Support multiple connection protocols.
(define (connect [host "127.0.0.1"] [port 28015] #:database [dbname #f])
  (define-values [in out] (tcp-connect host port))
  (write-bytes (integer->integer-bytes (v->int 'v0-4) 4 #f) out)
  (write-bytes (bytes #x00 #x00 #x00 #x00) out)
  (write-bytes (integer->integer-bytes (version-dummy:protocol->integer 'json) 4 #f) out)
  (flush-output out)
  (let ([response (read-null-term-str in)]
        [default (if dbname (db dbname) #f)])
    (if (string=? response "SUCCESS")
        (list in out default)
        '()))) ; empty list means invalid connection

;; Close the ports opened by connect.
(define (close-connection connection)
  (close-input-port (first connection))
  (close-output-port (second connection)))

;; *************************************************************************************************
;; *************************************************************************************************

;; A macro for defining an "anchored" operation. These are DB operations that can occur at the start
;; of a chain, e.g. db, db-create, object, etc.). These have no optional arguments in the RDB spec.
;;
;; This macro takes an opname (this will be something like "db", "db-create", etc.) and generates a
;; macro with that name. The generated macro takes a list of arguments (the "(... ...)" escapes the
;; ellipses so they can be used in the nested macro; they should be read as just normal ellipses), and
;; puts those args into a list that's passed to a reql-term constructor. (See reql-term.rkt.) This
;; reql-term is passed to a builder function which will build the rest of the query from this term.
;; Thus, each DB operation, like (db "library"), is a macro call that expands to a lambda whose
;; argument is a function that knows how to build the rest of the query.
(define-syntax-rule (defn-op-anchored opname)
  (define-syntax-rule (opname arg (... ...))
    (λ (builder)
      (builder (reql-term (term->int 'opname) (list arg (... ...)) '#hash())))))

;; A macro for defining a "chained" operation, i.e. one that is chained to something and may have
;; something chained to it. E.g., with the query db("library").table("books").filter(...), table
;; and filter are chained operations.
;;
;; How this works is very similar to above, but since these operations are chained, they first need to
;; take the reql-term produced by their predecessor and build a new term from that, then pass the
;; resulting term on to their successor operation, via the builder function. Thus, these macros expand
;; to a lambda that expects a reql-term (called "chained" below), and which then produces a lambda that
;; expects a builder as above. Think of the (λ (chained) ...) expression at one "level" as equivalent to
;; the builder argument from the previous level.
;;
;; The two different clauses in syntax-case handle the case with and without optional arguments.
;; TODO: The way this is currently written, operations that don't take optional arguments according to
;; the spec can still be passed optional arguments without complaint from the library. This should be fixed.
(define-syntax-rule (defn-op-chained opname)
  (define-syntax (opname stx)
    (syntax-case stx (with)
      [(_ arg (... ...) with opt)
       #'(λ (chained)
           (λ (builder)
             (builder (reql-term (term->int 'opname) (list chained arg (... ...)) opt))))]
      [(_ arg (... ...))
       #'(λ (chained)
           (λ (builder)
             (builder (reql-term (term->int 'opname) (list chained arg (... ...)) '#hash()))))])))


;; Helper macros to define a bunch of operations at once. Yay for ellipses!

(define-syntax-rule (define-anchored-ops op ...)
  (begin (defn-op-anchored op) ...))

(define-syntax-rule (define-chained-ops op ...)
  (begin (defn-op-chained op) ...))

;; *************************************************************************************************
;; *************************************************************************************************

;; I may be misinterpreting the spec, but this seems to deviate from the protobuf file. However, passing
;; data "plainly" works, while serializing as a datum type structure doesn't seem to.
(define (datum arg) (λ (builder) (builder arg)))

;; TODO: Why is this not defined as an anchored op?
(define (array . args)
  (reql-term (term->int 'make-array) args '#hash()))

(define-anchored-ops
  object db db-create db-drop db-list)

(define-chained-ops
  table get get-all
  eq ne lt le gt ge not
  add sub mul div mod
  floor ceil round
  append prepend difference
  set-insert set-intersection set-union set-difference
  slice skip limit offsets-of contains
  get-field keys values has-fields with-fields pluck without merge
  between reduce map fold filter concat-map order-by distinct count is-empty union nth bracket
  inner-join outer-join eq-join zip range
  insert-at delete-at change-at splice-at
  coerce-to type-of
  update delete replace insert
  table-create table-drop table-list config wait reconfigure rebalance sync grant
  index-create index-drop index-list index-status index-wait index-rename
  set-write-hook get-write-hook
  funcall branch or and for-each)

;; *************************************************************************************************
;; *************************************************************************************************

;; This is all for serializing lambda expressions (passed to e.g filter, etc.). TODO: Clean this up?

(begin-for-syntax
  (define (make-args-hash alovars)
    (build-list (length alovars) (lambda (n) (list (list-ref alovars n) (reql-term 10 (list (add1 n)) '#hash())))))

  (define (find-in alop var)
    (foldr (lambda (x y) (if (equal? (car x) var) (cadr x) y)) #f alop))

  (define (array args)
    (reql-term 2 args '#hash()))

  (define (replace-vars vars expr)
    (cond
      [(null? expr) null]
      [(pair? expr) (cons (replace-vars vars (car expr)) (replace-vars vars (cdr expr)))]
      [else
       (let ([new-val (find-in vars expr)])
         (if new-val new-val expr))]))
)

(define-syntax (r:lambda stx)
  (syntax-case stx ()
    [(_ args body ...)
      (with-syntax*
        ([args-hash (datum->syntax stx (make-args-hash (syntax->datum #'args)))]
         [args-hash-keys (datum->syntax stx (array (build-list (length (syntax->datum #'args-hash)) add1)))]
         [(replaced ...) (datum->syntax stx (map (lambda (x) (replace-vars (syntax->datum #'args-hash) x)) (syntax->datum #'(body ...))))])
      #'(reql-term 69 (list args-hash-keys (build-query replaced ...)) '#hash()))]))

;; *************************************************************************************************
;; *************************************************************************************************

;; This takes a reql-term struct and serializes it as per the spec. See the RethinkDB driver guide for details.
(define (serialize-reql-term term)
  (define (serialize-datum val)
    (cond [(symbol? val) (if (eq? val 'null) (symbol->string val) ;; no quotes around null
                           (string-append "\"" (symbol->string val) "\""))]
          [(number? val) (number->string val)]
          [(hash? val) (string-append "{" (serialize-hash val) "}")]
          [else (string-append "\"" val "\"")]))
  (define (reql-datum? val)
    (or- (symbol? val) (number? val) (hash? val) (string? val)))
  (define (serialize-reql-term-list terms)
    (cond [(empty? terms) ""]
          [(empty? (rest terms)) (serialize-reql-term (first terms))]
          [else
            (string-append
              (serialize-reql-term (first terms))
              ","
              (serialize-reql-term-list (rest terms)))]))
  (define (serialize-hash h)
    (define tmp (hash-map h (λ (k v) (string-append (serialize-datum k) ":" (serialize-reql-term v)))))
    (foldr (λ (x y) (string-append x "," y)) (first tmp) (rest tmp)))
  (cond [(reql-datum? term) (serialize-datum term)]
        [else (string-append "["
                             (number->string (reql-term-type term))
                             (string-append ",[" (serialize-reql-term-list (reql-term-args term)) "]")
                             (if (zero? (hash-count (reql-term-optargs term))) ""
                                 (string-append ",{" (serialize-hash (reql-term-optargs term)) "}"))
                             "]")]))

;; *************************************************************************************************
;; *************************************************************************************************

;; Each query needs a unique token (so that the response can be matched). This is obviously TODO.
(define TOKEN 0)
(define (get-token) (begin (set! TOKEN (remainder (add1 TOKEN) (expt 2 64))) TOKEN))

;; *************************************************************************************************
;; *************************************************************************************************

;; Some structs for raising exceptions when the server response indicates an error.
(define-struct (client-error exn:fail) ()) 
(define-struct (compile-error exn:fail) ()) 
(define-struct (runtime-error exn:fail) ()) 

;; Send serialized query to output port.
(define (write-query token query out)
  (write-bytes (integer->integer-bytes token 8 #f) out)
  (write-bytes (integer->integer-bytes (string-length query) 4 #f) out)
  (write-bytes (string->bytes/utf-8 query) out)
  (flush-output out))

;; Send a "start" query to server. See spec for details.
(define (start-query query token connection)
  (let* ([type (number->string (query:query-type->integer 'start))]
         [query-with-start (string-append "[" type "," query ",{}]")]
         [out (second connection)])
    (write-query token query-with-start out)))

;; Send a "continue" query to server. See spec for details.
(define (continue-query token connection)
  (let* ([type (number->string (query:query-type->integer 'continue))]
         [query (string-append "[" type "," token "]")]
         [out (second connection)])
    (write-query token query out)))

;; Send a "stop" query to server. See spec for details.
(define (stop-query token connection)
  (let* ([type (number->string (query:query-type->integer 'stop))]
         [query (string-append "[" type "]")]
         [out (second connection)])
    (write-query token query out)))

;; Send a "no reply wait" to server. See spec for details.
(define (noreply-wait token connection)
  (let* ([type (number->string (query:query-type->integer 'noreply-wait))]
         [query (string-append "[" type "]")]
         [out (second connection)])
    (write-query token query out)))

;; Read the response from the server. See the spec for possible forms the response
;; can take (including errors).
(define (read-response token connection)
  (define (complete-response->stream rsp)
    (cond [(empty? rsp) empty-stream]
          [else (stream-cons (first rsp) (complete-response->stream (rest rsp)))]))
  (define (partial-response->stream rsp)
    (cond [(empty? rsp)
           (continue-query token connection)
           (read-response token connection)]
          [else (stream-cons (first rsp) (partial-response->stream rsp))]))
  (let ([in (first connection)])
    ;; query token (8 bytes)
    (if (= token (integer-bytes->integer (read-bytes 8 in) #f))
        (begin
          ;; response size (4 bytes)
          (read-bytes 4 in)
          ;; hash from json-encoded rsponse
          (let* ([rsp (read-json in)]
                 [type (hash-ref rsp 't)]
                 [data (hash-ref rsp 'r)])
            (cond [(= type (response:response-type->integer 'success-atom)) data]
                  [(= type (response:response-type->integer 'success-sequence)) (complete-response->stream data)]
                  [(= type (response:response-type->integer 'success-partial)) (partial-response->stream data)]
                  [(= type (response:response-type->integer 'client-error))
                            (raise (client-error (first data) (current-continuation-marks)))]
                  [(= type (response:response-type->integer 'compile-error))
                            (raise (compile-error (first data) (current-continuation-marks)))]
                  [(= type (response:response-type->integer 'runtime-error))
                            (raise (runtime-error (first data) (current-continuation-marks)))])))
        (error "Query token does not match"))))

;; *************************************************************************************************
;; *************************************************************************************************

;; These functions take the list of weird lambda expressions generated by the opname macros
;; and apply them in succession so that in the end we have a single reql-term struct containing
;; the whole query, which can then be serialized.

(define (build-query . commands)
  ((apply build-query/partial commands) (λ (x) x)))

(define (build-query/partial . commands)
  (cond [(empty? (rest commands)) (first commands)]
        [else (let ([chain ((first commands) (second commands))])
                (apply build-query/partial (cons chain (rest (rest commands)))))]))

(define (build-query-with-default default . args)
  (apply build-query (cons default args)))

(define (run-helper query connection)
  (let ([token (get-token)])
    (start-query (serialize-reql-term query) token connection)
    (read-response token connection)))

(define-syntax (run stx)
  (syntax-case stx (with)
    [(_ command ... with connection)
     #'(run-helper (if (third connection)
                       (build-query-with-default (third connection) command ...)
                       (build-query command ...)) connection)]))
