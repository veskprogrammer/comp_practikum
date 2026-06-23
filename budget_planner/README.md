# Budget Planner / Expense Tracker WebAssembly Application

## Project Overview

This project demonstrates how to build a complete web application using WebAssembly compiled from C. The Budget Planner application allows users to track their expenses by entering expense details, viewing them in a table, and seeing calculated totals by category.

### Features

- Add expense entries with date, category, amount, and description
- Display expenses in a dynamically updated table
- Calculate and show total expenses and totals by category
- Delete individual expense entries
- Clear all expense entries
- Responsive and user-friendly GUI

## Project Structure

The project consists of the following files:

- **main.c**: Core logic for the expense tracker implemented in C
- **index.html**: HTML structure for the user interface
- **app.js**: Custom JavaScript for DOM manipulation and event handling

## Technical Implementation

### Programming Language & Compilation

The application is built using C as the primary programming language and compiled to WebAssembly using Emscripten. The C code handles all data processing and calculations, while the HTML and JavaScript provide the user interface.

### Data Structures

The C code defines the following data structures:

- `ExpenseEntry`: Represents an individual expense with date, category, amount, and description
- `CategoryTotal`: Stores the total amount for each expense category

### Core Functionality

The C code implements the following functions:

- Add an expense entry
- Delete an expense entry by index
- Clear all expense entries
- Calculate total expenses
- Calculate totals by category

### WebAssembly Integration

The C code is compiled to WebAssembly using Emscripten, which generates JavaScript glue code to interface with the HTML UI. The application uses Emscripten's API to export C functions to JavaScript and to update the HTML UI when data changes.

## Compilation Instructions

To compile the project, you need to have Emscripten installed. Follow these steps:

1. Install Emscripten by following the instructions at [https://emscripten.org/docs/getting_started/downloads.html](https://emscripten.org/docs/getting_started/downloads.html)

2. Clone or download this project to your local machine

3. Navigate to the project directory in your terminal

4. Compile the C code to WebAssembly using the following command:

```bash
emcc main.c -o index.js -s WASM=1 -O2 -s EXPORTED_RUNTIME_METHODS='["stringToUTF8","UTF8ToString"]' -s EXPORTED_FUNCTIONS='["_main","_jsAddExpense","_jsDeleteExpense","_jsClearAllExpenses","_jsGetTotalExpenses","_jsGetExpenseCount","_jsGetCategoryCount","_getExpenseJSON","_getCategoryTotalJSON","_freeMemory","_malloc","_free"]' --shell-file index.html -s ALLOW_MEMORY_GROWTH=1
```

This command:
- Compiles `main.c` to WebAssembly
- Uses `index.html` as a template for the output HTML file
- Exports the necessary C functions to JavaScript
- Enables memory growth for dynamic memory allocation
- Optimizes the code with `-O2`

Ensure that the following files are in the same directory as `index.html`:
- `app.js`
- `index.js`
- The WebAssembly file (`.wasm`) generated during compilation

## Running the Application

To run the application, you need to serve the compiled files using a web server. You can use Python's built-in HTTP server:

```bash
python3 -m http.server 8000
```

Then open your web browser and navigate to `http://localhost:8000/` to use the application.

## Educational Value

This project demonstrates several important concepts:

- WebAssembly compilation from C using Emscripten
- Data structures and memory management in C
- Integration between C, WebAssembly, and JavaScript
- Dynamic UI updates based on data changes
- Form validation and error handling

Students will learn how to:
- Structure a WebAssembly project
- Define and manipulate data structures in C
- Export C functions to JavaScript
- Handle user input and update the UI
- Implement a practical application with real-world utility

## License

This project is provided for educational purposes and can be freely used and modified for academic purposes.

## Student Improvement

The project was extended with an additional UI feature: **category filtering and CSV export**.

What was changed:

- Added a new **Filter and Export** block to `index.html`.
- Added a category selector that filters visible expense rows without changing data stored in WebAssembly memory.
- Added a **Visible total** value that recalculates the total only for currently displayed rows.
- Added an **Export Visible CSV** button that exports the currently displayed rows to `budget_planner_visible_expenses.csv`.
- Updated `app.js` to read expense data from the WebAssembly module through the existing `_getExpenseJSON` function and to keep the original row index for correct deletion after filtering.

The improvement does not require changing `main.c`, because it reuses the existing WebAssembly API exported by the original C code.

## Recommended Build Command

The project already includes a `makefile`. After Emscripten is activated, run:

```bash
make clean
make
```

If `make` is not available, run the same build command manually:

```bash
emcc main.c -s WASM=1 -s ASSERTIONS=1 -s MODULARIZE=1 -s EXPORT_NAME="'BudgetPlanner'" \
-s EXPORTED_FUNCTIONS='["_main","_showHelloMessage","_jsAddExpense","_jsDeleteExpense","_jsClearAllExpenses","_jsGetTotalExpenses","_jsGetExpenseCount","_jsGetCategoryCount","_getExpenseJSON","_getCategoryTotalJSON","_freeMemory","_malloc","_free"]' \
-s EXPORTED_RUNTIME_METHODS='["ccall", "cwrap", "stringToUTF8", "UTF8ToString"]' \
-O3 --llvm-opts 2 --llvm-lto 1 -s ALLOW_MEMORY_GROWTH=1 -o index.js
```

To run the app locally:

```bash
python -m http.server 8000
```

Open in browser:

```text
http://localhost:8000/
```
