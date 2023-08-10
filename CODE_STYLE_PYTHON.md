# Python Coding Style

This document resumes a couple of points we try to embrace in our coding style on python. Some of these points take an opinionated side on a trade-off story.
The description will try to make that clear.

The driving motivation of this code style is to make your code more readable.

Readable is one word that hides several dimensions:

- the reader understands the intent very rapidly
- the reader can proofread. It can become confident that the code is correct very easily.

Noticing how the two are different should not require too much squinting.
Shoot for _proofreadability_.

## Code reviews

Do a pass on your own code before sending it for review to avoid wasting the review time.
Also, a trivial code style issues can come in the way and avoid spotting
deeper issues with the code.

As a reviewer, your first mission is proofreading. If you find a logical bug, feel good. You did an awesome job today.

Your second goal is to make sure the code quality stays high.

You can express "nitpicks": suggestions about some local aspect of the code that do not matter too much. Just prepend "nitpick:" to your comment.
You can also express an opinion/advice that you know is not universal.
Make sure you make it clear to the reviewee that it is fine to ignore the comment.

Do not use rhetorical questions... If you are 95% sure of something, there is no need to express it as a question.
Prefer `I believe this should be n+1` to `Shouldn't this be n+1?`.

The issue with rhetorical questions is that when you will have a genuine
question, reviewees may over interpret it as an affirmation.

As a reviewee, if you are not used to CRs, it can feel like an adversarial process. Relax. This is normal to end up with a lot of comments on your first few CRs.

You might feel like the comments are unjustified, try as much as possible to not feel frustrated.
If you want to discuss it, the best place is the chat, or maybe send a PR to modify this document.

But remember to pick your battles... If you think it does not matter much but it takes 2 secs to fix, just consider doing what is suggested by the reviewer or this style guide.

## Base code styling

If you want to run the basic code styling checks on python (isort, flake8, mypy) execute:

```
make python-code-lint
```

And will run over all python packages to check if its ok and fix possible errors

## Naming

Function and variable names are key for readability.

A good function name is often sufficient for the reader to build reasonable expectations of what it does.

If this implies long names, let's have very long names.

Trying to fit this rule has an interesting side effect.
Nobody likes to type long function names. It just feels ugly.
But these are frequently symptoms of a badly organized code, and it can
help spot refactoring opportunities.

Use lower case and underlines for variables, classes and funcitons.

## Explanatory variables

One incredibly powerful tool and simple tool to help make your code
more readable is to introduce explanatory variables.

Explanatory variables are intermediary variables that were not really
necessary, but make it possible -through their names- to convey their
semantics to the reader.

## Shadowing

As much as possible, do not use reuse the same variable name in a function.
It is never necessary, very rarely helpful and can hurt and mypy will complain if the type is different.

## Types

Allways type your functions and variables so its easyer to read and check by mypy.

## Early returns

We prefer early return.
Rather than chaining `else` statement, we prefer to isolate
corner case in short `if` statement to prevent nesting

## Comments

Inline comments in the code can be very useful to help the reader understand
the justification of a thorny piece of code.

## Tests

> Warning
> Below is a description of the ideas of our project testing strategy. It does NOT necessarily
> reflect the current state of things.

Test do not need to match the same quality as the original code.

When a bug is encountered, it is ok to introduce a test that seems weirdly
overfitted to the specific issue. A comment should then add a link to the issue.

Unit test should run fast, and if possible they should not do any IO.
Code should be structured to make unit testing possible.

### Not just for spotting regression

Our unit tests are not here just to spot regression. They are here to check the correctness of our code and that things are wired up correctly.

### Test file naming

1. Integration and unit tests should be separated into their own folders
2. Test file names should match the modules they are testing

Example:

- `nucliadb_utils/tests/unit/storages/test_pg.py` has tests that target `nucliadb_utils/storages/pg.py`


### Unit vs Integration tests

Limit the number of integration tests you write. Less is more. Integration tests are slow and difficult to maintain.

### Functional testing.

Testing all the architecture is complex and packages like `nucliadb_one` and `nucliadb_search` starts all dependency layers on a distributed way with the cluster discovery protocol, testing the real environment. Takes some time to fire all dependencies but provides a good real integration and functional test.

### Beware of too many complex shared fixtures
Shared fixtures are something that need to be supported across potentially many tests. As more logic is shoved into shared fixture, it is increasingly difficult to maintain.

Prefer local test file or test class fixtures over genericly shared fixtures.

### It's okay to repeat yourself

If there is a problem with a test, it needs to be easy to understand all the fixtures and code involved in the setup of the test.

The more layers of fixtures/setup you have, the more difficult it is to read and maintain a test because you can't quickly see what a test is actually doing.

Prefer repeating yourself instead of trying to maintain generic fixtures. That way, it's easier to grok individual tests and if a test needs to be adjusted or deleted and rewritten, it can be done without needing to adjust shared fixtures.

## async vs sync

Your async code should block for at most 500 microseconds.
If you are unsure whether your code blocks for 500 microseconds, or if it is a non-trivial question, it should run via `asyncio.run_in_executor`.
