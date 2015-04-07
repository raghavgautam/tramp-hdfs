;;; tramp-hdfs.el --- Tramp extension to access hadoop/hdfs file system in Emacs

;; Copyright (C) 2008-2014  The Tramp HDFS Developers
;;
;; Version 0.3.0
;; Author: Raghav Kumar Gautam <raghav@apache.org>
;; Keywords: tramp, emacs, hdfs, hadoop
;; Acknowledgements: Thanks to tramp-smb.el, tramp-sh.el for inspiration & code.
;;
;; Contains code from GNU Emacs <https://www.gnu.org/software/emacs/>,
;; released under the GNU General Public License version 3 or later.
;; You should have received a copy of the GNU General Public License
;; along with tramp-hdfs.  If not, see <http://www.gnu.org/licenses/>.
;;
;;; Commentary:
;; Access hadoop/hdfs over Tramp.
;; This program uses ssh to login to another machine that has hdfs client to access hdfs.
;; It then fires hdfs commands to do ls or fetch files.
;;
;; Configuration:
;;   Add the following lines to your .emacs:
;;
;;   (add-to-list 'load-path "<<directory containing tramp-hdfs.el>>")
;;   (require 'tramp-hdfs);;; Code:
;;
;; Usage:
;;   open /hdfs:root@node-1:tmp/ in emacs
;;   where root   is the user that you want to use for ssh
;;         node-1 is the name of the machine that has hdfs client
;;
;;; Code:

(require 'tramp)
(require 'tramp-sh)
(require 'time-date)
;; Pacify byte-compiler.
(eval-when-compile
  (require 'cl))

;; Define hdfs method ...
(defcustom hdfs-ls "hdfs dfs -ls"
  "hdfs list command to list file/dir"
  :group 'tramp
  :type 'string
  :version "24.3")

(defcustom hdfs-ls-one "hdfs dfs -ls -d"
  "hdfs list command to list one file/dir"
  :group 'tramp
  :type 'string
  :version "24.3")

(defcustom hdfs-cat "hdfs dfs -cat"
  "hdfs list command to list one file/dir"
  :group 'tramp
  :type 'string
  :version "24.3")

(defcustom hdfs-tail "hdfs dfs -tail"
  "hdfs list command to list one file/dir"
  :group 'tramp
  :type 'string
  :version "24.3")

(defcustom hdfs-bigfile-threshold (* 1024 1024)
  "hdfs list command to list one file/dir"
  :group 'tramp
  :type 'int
  :version "24.3")

(defcustom hdfs-del "hdfs dfs -rm"
  "hdfs delete command to delete one file"
  :group 'tramp
  :type 'string
  :version "24.3")

(defcustom hdfs-del-dir "hdfs dfs -rmdir"
  "hdfs delete command to delete one dir"
  :group 'tramp
  :type 'string
  :version "24.3")

(defcustom hdfs-del-dir-recursive "hdfs dfs -rm -rf "
  "hdfs delete command to delete one dir recursively"
  :group 'tramp
  :type 'string
  :version "24.3")

(defcustom hdfs-default-dir "/"
  "hdfs default directory"
  :group 'tramp
  :type 'string
  :version "24.3")

(defconst tramp-hdfs-method "hdfs"
  "Method to connect HDFS servers.")

;; ... and add it to the method list.
;;;###tramp-autoload
(add-to-list 'tramp-methods
  `(,tramp-hdfs-method
    (tramp-login-program        "ssh")
    (tramp-login-args           (("-l" "%u") ("-p" "%p") ("%c")
				 ("-e" "none") ("%h")))
    (tramp-async-args           (("-q")))
    (tramp-remote-shell         "/bin/bash")
    (tramp-remote-shell-args    ("-c"))
    (tramp-gw-args              (("-o" "GlobalKnownHostsFile=/dev/null")
				 ("-o" "UserKnownHostsFile=/dev/null")
				 ("-o" "StrictHostKeyChecking=no")))
    (tramp-default-port         22)
    (tramp-connection-timeout   10)
    ;;(tramp-tmpdir "/tmp/tramp")
    ))

;;;###autoload
(eval-after-load 'tramp
  '(tramp-set-completion-function "hdfs" tramp-completion-function-alist-ssh))

(defconst tramp-hdfs-errors
  (mapconcat
   'identity
   `("Connection\\( to \\S-+\\)? failed"
     "Read from server failed, maybe it closed the connection"
     "Call timed out: server did not respond"
     "\\S-+: command not found"
     ".*No such file or directory.*"
     ".*Permission denied:.*"
     ".*Can not create a Path from an empty string.*"
     "Failed to.*")
   "\\|")
  "Regexp for possible error strings of hdfs servers.")

;; New handlers should be added here.
(defconst tramp-hdfs-file-name-handler-alist
  '(;; `access-file' performed by default handler.
    ;; `byte-compiler-base-file-name' performed by default handler.
    (delete-directory . tramp-hdfs-handle-delete-directory)
    (delete-file . tramp-hdfs-handle-delete-file)
    ;; `diff-latest-backup-file' performed by default handler.
    (directory-file-name . tramp-handle-directory-file-name)
    (directory-files . tramp-hdfs-handle-directory-files)
    (directory-files-and-attributes . tramp-handle-directory-files-and-attributes)
    (dired-call-process . ignore)
    (dired-compress-file . ignore)
    (dired-uncache . tramp-handle-dired-uncache)
    (expand-file-name . tramp-hdfs-handle-expand-file-name)
    (file-accessible-directory-p . tramp-hdfs-handle-file-directory-p)
    (file-acl . ignore)
    (file-attributes . tramp-hdfs-handle-file-attributes)
    (file-directory-p .  tramp-hdfs-handle-file-directory-p)
    (file-executable-p . tramp-handle-file-exists-p)
    (file-exists-p . tramp-handle-file-exists-p)
    ;; `file-in-directory-p' performed by default handler.
    (file-local-copy . tramp-hdfs-handle-file-local-copy)
    (file-modes . tramp-handle-file-modes)
    (file-name-all-completions . tramp-hdfs-handle-file-name-all-completions)
    (file-name-as-directory . tramp-handle-file-name-as-directory)
    (file-name-completion . tramp-handle-file-name-completion)
    (file-name-directory . tramp-handle-file-name-directory)
    (file-name-nondirectory . tramp-handle-file-name-nondirectory)
    (file-newer-than-file-p . tramp-handle-file-newer-than-file-p)
    (file-notify-add-watch . tramp-handle-file-notify-add-watch)
    (file-notify-rm-watch . tramp-handle-file-notify-rm-watch)
    (file-ownership-preserved-p . ignore)
    (file-readable-p . tramp-handle-file-exists-p)
    (file-regular-p . tramp-handle-file-regular-p)
    (file-remote-p . tramp-handle-file-remote-p)
    ;; `file-selinux-context' performed by default handler.
    ;; `file-truename' performed by default handler.
    (file-writable-p . ignore)
    ;; `find-file-noselect' performed by default handler.
    ;; `get-file-buffer' performed by default handler.
    (insert-directory . tramp-hdfs-handle-insert-directory)
    (insert-file-contents . tramp-handle-insert-file-contents)
    (load . tramp-handle-load)
    (make-auto-save-file-name . ignore)
    (make-directory . ignore)
    (make-directory-internal . ignore)
    (make-symbolic-link . ignore)
    (rename-file . ignore)
    (set-file-acl . ignore)
    (set-file-modes . ignore)
    (set-file-selinux-context . ignore)
    (set-file-times . ignore)
    (set-visited-file-modtime . tramp-handle-set-visited-file-modtime)
    (shell-command . tramp-handle-shell-command)
    (start-file-process . ignore)
    (unhandled-file-name-directory . tramp-handle-unhandled-file-name-directory)
    (vc-registered . ignore)
    (verify-visited-file-modtime . tramp-handle-verify-visited-file-modtime)
    (write-region . ignore)
    )
  "Alist of handler functions for Tramp hdfs method.
Operations not mentioned here will be handled by the default Emacs primitives.")

;; It must be a `defsubst' in order to push the whole code into
;; tramp-loaddefs.el.  Otherwise, there would be recursive autoloading.
;;;###tramp-autoload
(defsubst tramp-hdfs-file-name-p (filename)
  "Check if it's a filename for hdfs servers."
  (string= (tramp-file-name-method (tramp-dissect-file-name filename))
	   tramp-hdfs-method))

;;;###tramp-autoload
(defun tramp-hdfs-file-name-handler (operation &rest args)
  "Invoke the hdfs related OPERATION.
First arg specifies the OPERATION, second arg is a list of arguments to
pass to the OPERATION."
  (when (and (car args)
	     (string-prefix-p "/hdfs:" (car args))
	     ;;remove frequent operations
	     )
    (tramp-debug-message (tramp-dissect-file-name (car args)) "operation %s args %s" operation args))
  (let ((fn (assoc operation tramp-hdfs-file-name-handler-alist)))
    (if fn
	(save-match-data (apply (cdr fn) args))
      (tramp-run-real-handler operation args))))

;;;###tramp-autoload
(add-to-list 'tramp-foreign-file-name-handler-alist
	     (cons 'tramp-hdfs-file-name-p 'tramp-hdfs-file-name-handler))


;; File name primitives.
(defun tramp-hdfs-handle-delete-directory (directory &optional recursive)
  "Like `delete-directory' for Tramp files."
  (setq directory (directory-file-name (expand-file-name directory)))
  (let ((del-command hdfs-del-dir))
    (when recursive
      (setq del-command hdfs-del-dir-recursive))
    (with-parsed-tramp-file-name directory nil
      ;; We must also flush the cache of the directory, because
      ;; `file-attributes' reads the values from there.
      (tramp-flush-file-property v (file-name-directory localname))
      (tramp-flush-directory-property v localname)
      (unless (tramp-hdfs-send-command
	       v (format
		  "%s \"%s\""
		  del-command
		  (tramp-hdfs-get-filename v)))
	;; Error.
	(with-current-buffer (tramp-get-connection-buffer v)
	  (goto-char (point-min))
	  (search-forward-regexp tramp-hdfs-errors nil t)
	  (tramp-error
	   v 'file-error "%s `%s'" (match-string 0) directory))))))

(defun tramp-hdfs-handle-delete-file (filename &optional _trash)
  "Like `delete-file' for Tramp files."
  (setq filename (expand-file-name filename))
  (when (file-exists-p filename)
    (with-parsed-tramp-file-name filename nil
      ;; We must also flush the cache of the directory, because
      ;; `file-attributes' reads the values from there.
      (tramp-flush-file-property v (file-name-directory localname))
      (tramp-flush-file-property v localname)
      (tramp-hdfs-send-command
	       v (concat hdfs-del  " \"" (tramp-hdfs-get-filename v) "\"" ))
	;; Error.
	(with-current-buffer (tramp-get-connection-buffer v)
	  (goto-char (point-min))
	  (when (search-forward-regexp tramp-hdfs-errors nil t)
	    (tramp-error
	     v 'file-error "%s `%s'" (match-string 0) filename))))))

(defun tramp-hdfs-handle-directory-files
  (directory &optional full match nosort)
  "Like `directory-files' for Tramp files."
  (let ((result (mapcar 'directory-file-name
			(file-name-all-completions "" directory)))
	res)
    ;; Discriminate with regexp.
    (when match
      (setq result
	    (delete nil
		    (mapcar (lambda (x) (when (string-match match x) x))
			    result))))
    ;; Append directory.
    (when full
      (setq result
	    (mapcar
	     (lambda (x) (format "%s/%s" directory x))
	     result)))
    ;; Sort them if necessary.
    (unless nosort (setq result (sort result 'string-lessp)))
    ;; Remove double entries.
    (dolist (elt result res)
      (add-to-list 'res elt 'append))))

(defun tramp-hdfs-handle-expand-file-name (name &optional dir)
  (with-parsed-tramp-file-name (if (tramp-connectable-p name) name dir) nil
    (tramp-hdfs-maybe-open-connection v)
    (tramp-sh-handle-expand-file-name name dir)))

(defun tramp-hdfs-handle-file-attributes (filename &optional id-format)
  "Like `file-attributes' for Tramp files."
  (unless id-format (setq id-format 'integer))
  (ignore-errors
    (with-parsed-tramp-file-name filename nil
      (with-tramp-file-property
	  v localname (format "file-attributes-%s" id-format)
	(tramp-hdfs-do-file-attributes-with-stat v id-format))))) ;11 file system number

;;(progn (tramp-cleanup-all-buffers) (tramp-cleanup-all-connections))
;;manually open /hdfs:root@node-1:/user/
(defun tramp-hdfs-do-file-attributes-with-stat (vec &optional id-format)
  "Implement `file-attributes' for Tramp files using stat command."
  (tramp-message
   vec 5 "file attributes with stat: %s" (tramp-file-name-localname vec))
  (tramp-hdfs-maybe-open-connection vec)
  (with-current-buffer (tramp-get-connection-buffer vec)
    (let* ((localname (tramp-hdfs-get-filename vec))
	   size id replication uid gid atime mtime ctime mode inode ignore)
      (when (zerop (length localname))
	(setq localname hdfs-default-dir))
      (when (tramp-hdfs-send-command
	     vec (concat hdfs-ls-one  " \"" localname "\"" ))

	(goto-char (point-min))
	(when (re-search-forward tramp-hdfs-errors nil t)
	  (tramp-error vec 'file-error "%s" (match-string 0)))

	;; Loop the listing.
	(while (not (eobp))
	  (if (looking-at
	       (concat "^\\([-d][-drwxt]\\{9\\}\\)"
		       "[ ]+\\([-0-9]+\\)"
		       "[ ]\\([_[:alnum:]]+\\)"
		       "[ ]+\\([_[:alnum:]]+\\)"
		       "[ ]+\\([[:digit:]]+\\)"
		       "[ ]+\\([[:digit:]]\\{4\\}\\)"
		       "[-]+\\([[:digit:]]\\{2\\}\\)"
		       "[-]+\\([[:digit:]]\\{2\\}\\)"
		       "[ ]+\\([[:digit:]]\\{2\\}\\)"
		       "[:]+\\([[:digit:]]\\{2\\}\\)"
		       "[ ]\\([^ ]+\\)"
		       ))
	      (setq mode        (match-string 1 )
		    replication (string-to-number (match-string 2 ))
		    uid         (match-string 3 )
		    gid         (match-string 4 )
		    size        (string-to-number (match-string 5 ))
		    mtime (encode-time
			   0
			   ;;atime stats			     
			   (string-to-number (match-string 10))
			   (string-to-number (match-string  9))
			   (string-to-number (match-string  8))
			   (string-to-number (match-string  7))
			   (string-to-number (match-string  6)))
		    ignore (match-string 11)
		    id     (if (string-equal (substring mode 0 1) "d") t
			     "file")))
	  (forward-line)))
      
      ;; Return the result.
					;(list id replication uid gid '(0 0) mtime '(0 0) size mode nil inode (tramp-get-device vec))
      (when mode (list id replication uid gid '(0 0) mtime '(0 0) size mode nil inode (tramp-get-device vec))))))

(defun tramp-hdfs-handle-file-directory-p (filename)
  "Like `file-directory-p' for Tramp files."
  (and (file-exists-p filename)
       (eq ?d (aref (nth 8 (file-attributes filename)) 0))))

(defun tramp-hdfs-use-tail-p (filesize)
  (if (and (> filesize hdfs-bigfile-threshold)
	   (yes-or-no-p (format "Warning: file size = %sB is larger than %sB - should I open tail of file? " (file-size-human-readable filesize) (file-size-human-readable hdfs-bigfile-threshold))))
      t
    nil))

(defun tramp-hdfs-handle-file-local-copy (filename)
  "Like `file-local-copy' for Tramp files."
  (with-parsed-tramp-file-name filename nil
    (unless (file-exists-p filename)
      (tramp-error
       v 'file-error
       "Cannot make local copy of non-existing file `%s'" filename))

    (let* ((size (nth 7 (file-attributes (file-truename filename))))
	   (hdfs-cat-cmd (if (tramp-hdfs-use-tail-p size) hdfs-tail
			   hdfs-cat))
	   (rem-enc (tramp-get-inline-coding v "remote-encoding" size))
	   (loc-dec (tramp-get-inline-coding v "local-decoding" size))
	   (tmpfile (tramp-compat-make-temp-file filename)))

      (condition-case err
	  (cond
	   ;; Use inline encoding for file transfer.
	   (rem-enc
	    (save-excursion
	      (with-tramp-progress-reporter
	       v 3
	       (format "Encoding remote file `%s' with `%s'" filename rem-enc)
	       (tramp-barf-unless-okay
		v (format rem-enc (concat " <(" hdfs-cat-cmd " " (tramp-shell-quote-argument localname) ")"))
		"Encoding remote file failed"))

	      (with-tramp-progress-reporter
		  v 3 (format "Decoding local file `%s' with `%s'"
			      tmpfile loc-dec)
		(if (functionp loc-dec)
		    ;; If local decoding is a function, we call it.
		    ;; We must disable multibyte, because
		    ;; `uudecode-decode-region' doesn't handle it
		    ;; correctly.
		    (with-temp-buffer
		      (set-buffer-multibyte nil)
		      (insert-buffer-substring (tramp-get-buffer v))
		      (funcall loc-dec (point-min) (point-max))
		      ;; Unset `file-name-handler-alist'.  Otherwise,
		      ;; epa-file gets confused.
		      (let (file-name-handler-alist
			    (coding-system-for-write 'binary))
			(write-region
			 (point-min) (point-max) tmpfile nil 'no-message)))

		  ;; If tramp-decoding-function is not defined for this
		  ;; method, we invoke tramp-decoding-command instead.
		  (let ((tmpfile2 (tramp-compat-make-temp-file filename)))
		    ;; Unset `file-name-handler-alist'.  Otherwise,
		    ;; epa-file gets confused.
		    (let (file-name-handler-alist
			  (coding-system-for-write 'binary))
		      (with-current-buffer (tramp-get-buffer v)
			(write-region
			 (point-min) (point-max) tmpfile2 nil 'no-message)))
		    (unwind-protect
			(tramp-call-local-coding-command
			 loc-dec tmpfile2 tmpfile)
		      (delete-file tmpfile2)))))

	      ;; Set proper permissions.
	      (set-file-modes tmpfile (tramp-default-file-modes filename))
	      ;; Set local user ownership.
	      (tramp-set-file-uid-gid tmpfile)))

	   ;; Oops, I don't know what to do.
	   (t (tramp-error
	       v 'file-error "Wrong method specification for `%s'" method)))

	;; Error handling.
	((error quit)
	 (delete-file tmpfile)
	 (signal (car err) (cdr err))))

      (run-hooks 'tramp-handle-file-local-copy-hook)
      tmpfile)))

(defun tramp-hdfs-handle-file-name-all-completions (filename directory)
  "Like `file-name-all-completions' for Tramp files."
  (all-completions
   filename
   (with-parsed-tramp-file-name directory nil
     (with-tramp-file-property v localname "file-name-all-completions"
       (save-match-data
	 (let ((entries (tramp-hdfs-get-file-entries directory)))
	   (mapcar
	    (lambda (x)
	      (list
	       (if (string-match "d" (nth 1 x))
		   (file-name-as-directory (nth 0 x))
		 (nth 0 x))))
	    entries)))))))

(defun tramp-hdfs-handle-insert-directory
    (filename switches &optional wildcard full-directory-p)
  "Like `insert-directory' for Tramp files."
  (setq filename (expand-file-name filename))
  (unless switches (setq switches ""))
  (if full-directory-p
      ;; Called from `dired-add-entry'.
      (setq filename (file-name-as-directory filename))
    (setq filename (directory-file-name filename)))
  (with-parsed-tramp-file-name filename nil
    (save-match-data
      ;;we don't know what to do with switches and other options
      (let ((cur-buf (current-buffer))
	    dired-content
	    mode replication uid gid size mtime fullname)
	(when (tramp-hdfs-send-command
	       v (concat hdfs-ls  " \"" (tramp-hdfs-get-filename v) "\"" ))
	  (with-current-buffer (tramp-get-connection-buffer v)
	    ;; Loop the listing.
	    (goto-char (point-min))
	    (when (re-search-forward tramp-hdfs-errors nil t)
	      (tramp-error v 'file-error "%s" (match-string 0)))
	    (while (not (eobp))
	      (beginning-of-line)
	      (setq dired-content "")
	      (if (looking-at
		   (concat "^\\([-d][-drwxt]\\{9\\}\\)"
			   "[ ]+\\([-0-9]+\\)"
			   "[ ]\\([-_[:alnum:]]+\\)"
			   "[ ]+\\([-_[:alnum:]]+\\)"
			   "[ ]+\\([[:digit:]]+\\)"
			   "[ ]+\\([[:digit:]]\\{4\\}\\)"
			   "[-]+\\([[:digit:]]\\{2\\}\\)"
			   "[-]+\\([[:digit:]]\\{2\\}\\)"
			   "[ ]+\\([[:digit:]]\\{2\\}\\)"
			   "[:]+\\([[:digit:]]\\{2\\}\\)"
			   "[ ]\\([^ ]+\\)$"))
		  (progn (setq mode        (match-string 1)
			       replication (match-string 2)
			       uid         (match-string 3)
			       gid         (match-string 4)
			       size        (match-string 5)
			       fullname (match-string 11)
			       mtime (encode-time
				      0
				      ;;mtime stats			     
				      (string-to-number (match-string 10))
				      (string-to-number (match-string  9))
				      (string-to-number (match-string  8))
				      (string-to-number (match-string  7))
				      (string-to-number (match-string  6)))
					;		      id     (if (string-equal (substring mode 0 1) "d") t "file")
			       )
			 (setq dired-content
			       (concat
				dired-content
				(format
				 "%10s %3s %-10s %-10s %8s %s "
				 (or mode "----------") ; mode
				 (or replication "-") ; inode
				 (or uid "nobody") ; uid
				 (or gid "nogroup") ; gid
				 (or size "0") ; size
				 (format-time-string
				  (if (time-less-p
				       (time-subtract (current-time) mtime)
				       tramp-half-a-year)
				      "%b %e %R"
				    "%b %e  %Y")
				  mtime))))
			 (let ((start (point)))
			   (setq
			    dired-content
			    (concat
			     dired-content
			     (format	"%s\n" (file-name-nondirectory fullname)))
					;(put-text-property start (1- (point)) 'dired-filename t) ;;can use propertizie
			    )))
		;;insert the line
		(setq dired-content
		      (concat dired-content (buffer-substring (point) (point-at-eol)) "\n")))
	      (forward-line)
	      (with-current-buffer cur-buf (insert dired-content)))))))))

;; Internal file name functions.
(defun tramp-hdfs-get-share (vec)
  "Returns the share name of LOCALNAME."
  ;;(throw nil "this does not make sense for hdfs")
  (save-match-data
    (let ((localname (tramp-file-name-localname vec)))
      (when (string-match "^/?\\(.+\\)/" localname)
	(match-string 1 localname)))))

(defun tramp-hdfs-get-filename (vec)
  "Returns the file name of LOCALNAME.
If VEC has no cifs capabilities, exchange \"/\" by \"\\\\\"."
  (elt vec 3))

(defun tramp-hdfs-get-file-entries (directory)
  "Read entries which match DIRECTORY.
Either the shares are listed, or the `dir' command is executed.
Result is a list of (LOCALNAME MODE SIZE MONTH DAY TIME YEAR)."
  (with-parsed-tramp-file-name (file-name-as-directory directory) nil
    (when (zerop (length localname))
      (setq localname hdfs-default-dir))
    (with-tramp-file-property v localname "file-entries"
      (with-current-buffer (tramp-get-connection-buffer v)
	(let* (;;(share (tramp-hdfs-get-share v))
	       ;;(cache (tramp-get-connection-property v "share-cache" nil))
	       (res nil)
	       (entry nil))

	  ;;(if (and (not share) cache)
	      ;; Return cached shares.
	    ;;  (setq res cache)

	    ;; Read entries.
	    (tramp-hdfs-send-command
	    ;;(tramp-send-string
	     v (format (concat hdfs-ls " \"%s\"") (elt v 3)))
	    ;; Loop the listing.
	    (goto-char (point-min))
	    (current-buffer)
	    (if ;;(ignore-errors
		  (re-search-forward tramp-hdfs-errors nil t)
		;;)
		(tramp-error v 'file-error "%s `%s'" (match-string 0) directory)
	      (while (not (eobp))
		(setq entry (tramp-hdfs-read-file-entry v))
		(forward-line)
		(when entry (push entry res))))

	    ;; Cache share entries.
	    ;;(unless share
	      ;;(tramp-set-connection-property v "share-cache" res)))

	  ;; Add directory itself.
	  ;;(push '("" "drwxrwxrwx" 0 (0 0)) res)

	  ;; Return entries.
	  (delq nil res))))))


(defun tramp-hdfs-read-file-entry (vec)
  "Parse entry in hdfs output buffer.
Result is the list (FNAME MODE SIZE MTIME)."
;; We are called from `tramp-hdfs-get-file-entries', which sets the
;; current buffer.
  (let* ((basepath (elt vec 3))
	 (line (buffer-substring (point) (point-at-eol)))
	 (basedir-name (if (string-match "/$" basepath)
			   basepath
			 (concat basepath "/")))
	 (basedir-offset (if (string-equal basedir-name "./")
			     0
			   (length basedir-name)))
	 fullname file-name nlinks owner group mode size mtime)
    ;; Real listing.
    (if (string-match
	 (concat "^\\([-d][-drwxt]\\{9\\}\\)"
		 "[ ]+\\([-0-9]+\\)"
		 "[ ]\\([-_[:alnum:]]+\\)"
		 "[ ]+\\([-_[:alnum:]]+\\)"
		 "[ ]+\\([[:digit:]]+\\)"
		 "[ ]+\\([[:digit:]]\\{4\\}\\)"
		 "[-]+\\([[:digit:]]\\{2\\}\\)"
		 "[-]+\\([[:digit:]]\\{2\\}\\)"
		 "[ ]+\\([[:digit:]]\\{2\\}\\)"
		 "[:]+\\([[:digit:]]\\{2\\}\\)"
		 "[ ]\\([^ ]+\\)$"
		 )
	 line)
	(setq mode   (match-string 1  line)
	      nlinks (match-string 2  line)
	      owner  (match-string 3  line)
	      group  (match-string 4  line)
	      size   (when (match-string 5 line)
		       (string-to-number (match-string 5 line)))
	      mtime (encode-time
		     0
		     ;;atime stats			     
		     (string-to-number (match-string 10))
		     (string-to-number (match-string  9))
		     (string-to-number (match-string  8))
		     (string-to-number (match-string  7))
		     (string-to-number (match-string  6)))
	      fullname  (match-string 11 line)
	      file-name (substring fullname basedir-offset)))
    (when (and fullname mode size)
      (when (and (string-match "^d" mode)
		 (not (string-match "/$" file-name)))
	(setq file-name (concat file-name "/")))
      (tramp-set-file-property
       vec fullname "file-attributes-integer"
       (list (if (string-equal (substring mode 0 1) "d") t "file")
	     nlinks owner group '(0 0) mtime '(0 0) size mode nil nil (tramp-get-device vec)))
      (list file-name mode size mtime))))

(defun tramp-hdfs-send-command (vec command &optional neveropen nooutput)
  "Send the COMMAND to connection VEC.
Erases temporary buffer before sending the command.  If optional
arg NEVEROPEN is non-nil, never try to open the connection.  This
is meant to be used from `tramp-maybe-open-connection' only.  The
function waits for output unless NOOUTPUT is set."
  (unless neveropen (tramp-hdfs-maybe-open-connection vec))
  (let ((p (tramp-get-connection-process vec)))
    (when (tramp-get-connection-property p "remote-echo" nil)
      ;; We mark the command string that it can be erased in the output buffer.
      (tramp-set-connection-property p "check-remote-echo" t)
      (setq command (format "%s%s%s" tramp-echo-mark command tramp-echo-mark)))
    ;; Send the command.
    (tramp-message vec 6 "%s" command)
    (with-tramp-progress-reporter
	vec 3
	(format "Running command `%s'" command)
      (tramp-send-string vec command)
      (unless nooutput (tramp-wait-for-output p)))))

(defun tramp-hdfs-maybe-open-connection (vec)
  "Maybe open a connection to HOST, log in as USER"
  (when (let ((tramp-remote-path nil)) ;;tramp-remote-path does not make sense for hdfs
	  (tramp-maybe-open-connection vec))
    (tramp-message vec 5 "Setting $PATH environment variable")
    (tramp-hdfs-send-command
     vec
     (format (concat "PATH=" (mapconcat 'identity (cdr tramp-remote-path) ":") "; export PATH")
	       ;;(mapconcat 'identity (cdr tramp-remote-path) ":")
	     )
     t)))

(add-hook 'tramp-unload-hook
	  (lambda ()
	    (unload-feature 'tramp-hdfs 'force)))

(provide 'tramp-hdfs)

;;; tramp-hdfs.el ends here
