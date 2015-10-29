;;; tramp-hdfs-tests.el --- Tests for tramp-hdfs.

;; Copyright (C) 2008-2014  The Tramp HDFS Developers
;;
;; Version 0.3.0
;; Author: Raghav Kumar Gautam <raghav@apache.org>
;; Keywords: tramp, emacs, hdfs, hadoop, webhdfs, rest
;; Acknowledgements: Thanks to tramp-smb.el, tramp-sh.el for inspiration & code.
;;
;; Contains code from GNU Emacs <https://www.gnu.org/software/emacs/>,
;; released under the GNU General Public License version 3 or later.
;; You should have received a copy of the GNU General Public License
;; along with tramp-hdfs.  If not, see <http://www.gnu.org/licenses/>.

;;; Commentary:
;; These are some tests for tramp-hdfs.
;;
;;; Code:
(require 'tramp-hdfs)
(require 'ert)
(ert-deftest hdfs-test-expand-file-name1 ()
  "Tests the expand-file-name for hdfs."
  (should (equal (expand-file-name "/hdfs:node-1:"             "/tmp") "/hdfs:rgautam@node-1:/"))
  (should (equal (expand-file-name "/hdfs:root@node-1:"        "/Users") "/hdfs:root@node-1:/" ))
  (should (equal (expand-file-name "/hdfs:root@node-1:"        nil)      "/hdfs:root@node-1:/" ))
  (should (equal (expand-file-name "/hdfs:root@node-1:/"       "/Users") "/hdfs:root@node-1:/" ))
  (should (equal (expand-file-name "/hdfs:root@node-1:/"       nil)      "/hdfs:root@node-1:/" ))
  (should (equal (expand-file-name "/hdfs:root@node-1:/t"      "/Users") "/hdfs:root@node-1:/t"))
  (should (equal (expand-file-name "/hdfs:root@node-1:/t"      nil)      "/hdfs:root@node-1:/t"))
  (should (equal (expand-file-name "/hdfs:root@node-1://t"     "/Users") "/hdfs:root@node-1:/t"))
  (should (equal (expand-file-name "/hdfs:root@node-1://t"     nil)      "/hdfs:root@node-1:/t"))
  (should (equal (expand-file-name "/hdfs:root@node-1:/"       "/Users") "/hdfs:root@node-1:/" ))
  (should (equal (expand-file-name "/hdfs:root@node-1:/"       nil)      "/hdfs:root@node-1:/" )))
(ert-deftest hdfs-test-expand-file-name2 ()
  (should (equal (expand-file-name "/hdfs:root@node-1:./"      nil)      "/hdfs:root@node-1:/" ))
  (should (equal (expand-file-name "/hdfs:root@node-1:/."      nil)      "/hdfs:root@node-1:/" ))
  (should (equal (expand-file-name "/hdfs:root@node-1:/a/."   "/Users")  "/hdfs:root@node-1:/a"))
  (should (equal (expand-file-name "/hdfs:root@node-1:/a/."   nil)       "/hdfs:root@node-1:/a")))
(ert-deftest hdfs-test-expand-file-name3 ()
  (should (equal (expand-file-name "/hdfs:root@node-1:/.."   "/Users") "/hdfs:root@node-1:/.." ))
  (should (equal (expand-file-name "/hdfs:root@node-1:/.."   nil)      "/hdfs:root@node-1:/.." ))
  (should (equal (expand-file-name "/hdfs:root@node-1:/a/.."   "/Users") "/hdfs:root@node-1:/" ))
  (should (equal (expand-file-name "/hdfs:root@node-1:/a/.."   nil)      "/hdfs:root@node-1:/" ))
  (should (equal (expand-file-name "/hdfs:root@node-1:/a/../x" "/Users") "/hdfs:root@node-1:/x"))
  (should (equal (expand-file-name "/hdfs:root@node-1:/a/../x" nil)      "/hdfs:root@node-1:/x")))
(ert-deftest hdfs-test-expand-file-name4 ()
  (should (equal (expand-file-name "/hdfs:root@node-1:/a//x" "/Users") "/hdfs:root@node-1:/a/x"))
  (should (equal (expand-file-name "/hdfs:root@node-1:/a//x" nil)      "/hdfs:root@node-1:/a/x")))
(ert-deftest hdfs-test-expand-file-name5 ()
  (should (equal (expand-file-name "/hdfs:root@node-1:/a/~" "/Users") "/hdfs:root@node-1:/a/~"))
  (should (equal (expand-file-name "/hdfs:root@node-1:/a/~" nil)      "/hdfs:root@node-1:/a/~")))

;;file-attributes
(ert-deftest hdfs-test-file-attributes ()
  (should (equal (null (file-attributes "/hdfs:root@node-1://tmp")) (null (file-attributes "/ssh:root@node-1://tmp"))))
  (should (equal (null (file-attributes "/hdfs:root@node-1:/."))    (null (file-attributes "/ssh:root@node-1:/.")))))

;;directory-files
(ert-deftest hdfs-test-directory-files ()
  (should (equal (directory-files "/hdfs:root@node-1:/")  '("app-logs" "apps" "hdp" "mapred" "mr-history" "tmp" "user")))
  (should (equal (directory-files "/hdfs:root@node-1:")   '("app-logs" "apps" "hdp" "mapred" "mr-history" "tmp" "user"))))


;;file-name-all-completions
;;This function should return "foo/" for directories and "bar" for files.
(ert-deftest hdfs-test-file-name-completions ()
  (should (equal (file-name-all-completions "" "/hdfs:root@node-1:/") '("app-logs/" "apps/" "hdp/" "mapred/" "mr-history/" "tmp/" "user/")))
  (should (equal (file-name-all-completions "" "/hdfs:root@node-1:/tmp/id.out") '("_SUCCESS" "part-m-00000"))))

(ert-deftest hdfs-test-file-name-completions2 ()
  (should (equal (file-name-all-completions "app" "/hdfs:root@node-1:/") '("app-logs/" "apps/")))
  (should (equal (file-name-all-completions "h" "/hdfs:root@node-1:/tmp/") '("hive/")))
  (should (equal (file-name-all-completions "p" "/hdfs:root@node-1:/tmp/id.out/") '("part-m-00000")))
  (should (equal (file-name-all-completions "hi" "/hdfs:root@node-1:/tmp/") '("hive/")))
  (should (equal (file-name-all-completions "pa" "/hdfs:root@node-1:/tmp/id.out/") '("part-m-00000"))))

;;file-directory-p
(ert-deftest hdfs-test-file-directory-p ()
  (should (equal (file-directory-p "/hdfs:root@node-1:/tmp/id.out/_SUCCESS") nil))
  (should (equal (file-directory-p "/hdfs:root@node-1:/tmp/") t))
  (should (equal (file-directory-p "/hdfs:root@node-1:/tmp") t)))

;;file-attributes
(ert-deftest hdfs-test-file-attributes ()
  (should (null (file-attributes "/hdfs:root@node-1:/non-existing/")))
  (should (null (file-attributes "/hdfs:root@node-1:/non-existing")))
  (should (file-attributes "/hdfs:root@node-1:/"))
  (should (equal (first (file-attributes "/hdfs:root@node-1:/")) t))
  (should (equal (first (file-attributes "/hdfs:root@node-1:/tmp/id.out/part-m-00000"))nil))
  ;;other values of file attributes take different value for each execution
  ;;hence they need to checked manually
  )

;;TODO implement write support
;;file-writable-p
;;(file-writable-p "/hdfs:root@node-1:/tmp/id.out/part-m-00000")
;;(file-writable-p "/tmp")
;;(file-writable-p "/hdfs:root@node-1:/tmp/id.out/_SUCCESS")
;;(file-writable-p "/hdfs:root@node-1:/tmp/")
;;(file-writable-p "/hdfs:root@node-1:/non-existing/")

;;list-directory
(ert-deftest hdfs-test-list-directory ()
  (should (tramp-hdfs-list-directory (tramp-dissect-file-name "/hdfs:root@node-1:/tmp/id.out/")))
  (should (tramp-hdfs-list-directory (tramp-dissect-file-name "/hdfs:root@node-1:/tmp/id.out/")))
  (should (tramp-hdfs-list-directory (tramp-dissect-file-name "/hdfs:root@node-1:/tmp/id.out")))
  (should (tramp-hdfs-list-directory (tramp-dissect-file-name "/hdfs:root@node-1:/tmp/id.out/_SUCCESS")))
  (should-error (tramp-hdfs-list-directory  (tramp-dissect-file-name "/hdfs:root@node-1:/non"))
		:type 'file-error))

;;(insert-directory "/hdfs:node-1:/" "--dired -al" nil t)

;;big-file-warning
;;(let ((hdfs-bigfile-threshold 0))
;;  (find-file "/hdfs:root@node-1:/tmp/id.out/part-m-00000"))

;;find-file
(ert-deftest hdfs-test-find-file ()
  (unwind-protect
      (progn
	(should (equal (buffer-live-p (find-file "/hdfs:root@node-1:/")) t))
	(should (equal (buffer-live-p (find-file "/hdfs:root@node-1:/")) t))
	(should (equal (buffer-live-p (find-file "/hdfs:node-1:/tmp/id.out/part-m-00000")) t)))
    (progn (tramp-cleanup-all-connections) (tramp-cleanup-all-buffers))))

;;(let ((tramp-verbose 10)) (find-file "/hdfs:root@node-1:/"))
(ert "hdfs-test*")
(provide 'tramp-hdfs-tests)
;;; tramp-hdfs-tests.el ends here
