docs:
		python3 -m socrata.docs
		markdown-toc -i README.md


clean:
	rm -rf docs
