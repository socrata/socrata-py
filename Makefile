docs:
	# This is terrible but i can't think of a better way to accomplish
	# this
	echo "Installing newest version"
	python setup.py install
	$(eval PREV_BRANCH := $(shell git branch | grep \* | cut -d ' ' -f2))
	git checkout gh-pages
	pdoc socrata --html --overwrite --html-dir docs
	cp docs/socrata/* docs
	rm -r docs/socrata
	git add docs
	git commit -m 'Update docs'
	git push origin gh-pages
	git checkout $(PREV_BRANCH)

clean:
	rm -rf docs
