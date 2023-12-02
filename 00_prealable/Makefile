# define the name of the virtual environment directory
VENV := venv

# venv is a shortcut target
install:
	. $(VENV)/bin/activate && pip install -r requirements.txt

venv:
	test -d venv || python3 -m venv venv

run: venv
	. $(VENV)/bin/activate && ( \
		$(VENV)/bin/python ./inserter.py; \
	)

clean:
	rm -rf $(VENV)
	find . -type f -name '*.pyc' -delete

.PHONY: all venv run clean
