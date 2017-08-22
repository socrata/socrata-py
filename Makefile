docs:
	# This is terrible but i can't think of a better way to accomplish
	# this
	echo "Installing newest version"
	virtualenv -p python3 venv
	venv/bin/python3 setup.py install
	$(eval PREV_BRANCH := $(shell git branch | grep \* | cut -d ' ' -f2))
	git checkout gh-pages
	chmod +x venv/bin/activate
	./venv/bin/activate
	pdoc socrata --html --overwrite --html-dir docs
	cp docs/socrata/* docs
	rm -r docs/socrata
	git add docs
	git commit -m 'Update docs'
	git push origin gh-pages
	git checkout $(PREV_BRANCH)

clean:
	rm -rf docs
