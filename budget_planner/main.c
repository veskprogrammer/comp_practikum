/**
 * main.c - Budget Planner / Expense Tracker WebAssembly Application
 * 
 * This file implements the core logic for a budget planner application that allows users
 * to track their expenses. The C code is compiled to WebAssembly using Emscripten and
 * interfaces with an HTML/JavaScript frontend.
 * 
 * Features:
 * - Add expense entries with date, category, amount, and description
 * - Delete individual expense entries
 * - Clear all expense entries
 * - Calculate total expenses and totals by category
 * 
 * Author: University Professor
 * For: Computer Practicum Course - 2nd Year IT Bachelor Students
 * Date: March 2025
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <emscripten.h>

/**
 * Maximum number of expense entries the application can store
 * This could be made dynamic with reallocation, but a fixed size is used for simplicity
 */
#define MAX_EXPENSES 100

/**
 * Maximum length for string fields in the expense entry
 */
#define MAX_STRING_LENGTH 100

/**
 * Maximum number of different expense categories
 */
#define MAX_CATEGORIES 10

// Forward declarations of functions
void updateCategoryTotals();
void updateUI();

/**
 * Structure to represent an individual expense entry
 */
typedef struct {
    char date[MAX_STRING_LENGTH];      // Date of the expense (stored as string for simplicity)
    char category[MAX_STRING_LENGTH];  // Category of the expense (e.g., "Food", "Transport")
    double amount;                     // Amount of the expense
    char description[MAX_STRING_LENGTH]; // Description of the expense
} ExpenseEntry;

/**
 * Structure to store category totals
 */
typedef struct {
    char name[MAX_STRING_LENGTH];      // Category name
    double total;                      // Total amount for this category
} CategoryTotal;

/**
 * Global array to store all expense entries
 */
ExpenseEntry expenses[MAX_EXPENSES];

/**
 * Global array to store category totals
 */
CategoryTotal categoryTotals[MAX_CATEGORIES];

/**
 * Current number of expense entries stored
 */
int expenseCount = 0;

/**
 * Current number of unique categories
 */
int categoryCount = 0;

/**
 * Function to add a new expense entry
 * 
 * @param date The date of the expense
 * @param category The category of the expense
 * @param amount The amount of the expense
 * @param description The description of the expense
 * @return 1 if successful, 0 if the expenses array is full
 */
int addExpense(const char* date, const char* category, double amount, const char* description) {
    // Check if we have reached the maximum number of expenses
    if (expenseCount >= MAX_EXPENSES) {
        return 0; // Failure - array is full
    }
    
    // Create a new expense entry
    ExpenseEntry* newExpense = &expenses[expenseCount];
    
    // Copy the data into the new expense entry
    strncpy(newExpense->date, date, MAX_STRING_LENGTH - 1);
    newExpense->date[MAX_STRING_LENGTH - 1] = '\0'; // Ensure null termination
    
    strncpy(newExpense->category, category, MAX_STRING_LENGTH - 1);
    newExpense->category[MAX_STRING_LENGTH - 1] = '\0'; // Ensure null termination
    
    newExpense->amount = amount;
    
    strncpy(newExpense->description, description, MAX_STRING_LENGTH - 1);
    newExpense->description[MAX_STRING_LENGTH - 1] = '\0'; // Ensure null termination
    
    // Increment the expense count
    expenseCount++;
    
    // Update the category totals
    updateCategoryTotals();
    
    // Update the UI
    updateUI();
    
    return 1; // Success
}

/**
 * Function to delete an expense entry by its index
 * 
 * @param index The index of the expense to delete
 * @return 1 if successful, 0 if the index is invalid
 */
int deleteExpense(int index) {
    // Check if the index is valid
    if (index < 0 || index >= expenseCount) {
        return 0; // Failure - invalid index
    }
    
    // Shift all expenses after the deleted one one position to the left
    for (int i = index; i < expenseCount - 1; i++) {
        expenses[i] = expenses[i + 1];
    }
    
    // Decrement the expense count
    expenseCount--;
    
    // Update the category totals
    updateCategoryTotals();
    
    // Update the UI
    updateUI();
    
    return 1; // Success
}

/**
 * Function to clear all expense entries
 * 
 * @return 1 indicating success
 */
int clearAllExpenses() {
    // Reset the expense count to 0
    expenseCount = 0;
    
    // Reset the category count to 0
    categoryCount = 0;
    
    // Update the UI
    updateUI();
    
    return 1; // Success
}

/**
 * Function to calculate the total of all expenses
 * 
 * @return The total amount of all expenses
 */
double calculateTotalExpenses() {
    double total = 0.0;
    
    // Sum up all expense amounts
    for (int i = 0; i < expenseCount; i++) {
        total += expenses[i].amount;
    }
    
    return total;
}

/**
 * Function to update the category totals
 * This recalculates all category totals from scratch
 */
void updateCategoryTotals() {
    // Reset the category count
    categoryCount = 0;
    
    // Clear all category totals
    for (int i = 0; i < MAX_CATEGORIES; i++) {
        categoryTotals[i].name[0] = '\0';
        categoryTotals[i].total = 0.0;
    }
    
    // Calculate totals for each category
    for (int i = 0; i < expenseCount; i++) {
        const char* category = expenses[i].category;
        double amount = expenses[i].amount;
        
        // Check if this category already exists in our totals
        int categoryIndex = -1;
        for (int j = 0; j < categoryCount; j++) {
            if (strcmp(categoryTotals[j].name, category) == 0) {
                categoryIndex = j;
                break;
            }
        }
        
        // If the category doesn't exist yet, add it
        if (categoryIndex == -1) {
            // Check if we have reached the maximum number of categories
            if (categoryCount >= MAX_CATEGORIES) {
                // Too many categories, can't add more
                continue;
            }
            
            categoryIndex = categoryCount;
            strncpy(categoryTotals[categoryIndex].name, category, MAX_STRING_LENGTH - 1);
            categoryTotals[categoryIndex].name[MAX_STRING_LENGTH - 1] = '\0'; // Ensure null termination
            categoryTotals[categoryIndex].total = 0.0;
            categoryCount++;
        }
        
        // Add the expense amount to the category total
        categoryTotals[categoryIndex].total += amount;
    }
}

/**
 * Function to get the number of expense entries
 * 
 * @return The number of expense entries
 */
int getExpenseCount() {
    return expenseCount;
}

/**
 * Function to get the number of unique categories
 * 
 * @return The number of unique categories
 */
int getCategoryCount() {
    return categoryCount;
}

/**
 * Function to get an expense entry by its index
 * This function is exported to JavaScript to access expense data
 * 
 * @param index The index of the expense to get
 * @return A pointer to a string containing the expense data in JSON format
 *         The caller is responsible for freeing this memory
 */
char* EMSCRIPTEN_KEEPALIVE getExpenseJSON(int index) {
    // Check if the index is valid
    if (index < 0 || index >= expenseCount) {
        return NULL;
    }
    
    // Get the expense at the specified index
    ExpenseEntry* expense = &expenses[index];
    
    // Allocate memory for the JSON string
    // Format: {"date":"2025-03-21","category":"Food","amount":10.50,"description":"Lunch"}
    char* json = (char*)malloc(MAX_STRING_LENGTH * 4);
    if (json == NULL) {
        return NULL;
    }
    
    // Format the expense data as JSON
    sprintf(json, "{\"date\":\"%s\",\"category\":\"%s\",\"amount\":%.2f,\"description\":\"%s\"}",
            expense->date, expense->category, expense->amount, expense->description);
    
    return json;
}

/**
 * Function to get a category total by its index
 * This function is exported to JavaScript to access category total data
 * 
 * @param index The index of the category total to get
 * @return A pointer to a string containing the category total data in JSON format
 *         The caller is responsible for freeing this memory
 */
char* EMSCRIPTEN_KEEPALIVE getCategoryTotalJSON(int index) {
    // Check if the index is valid
    if (index < 0 || index >= categoryCount) {
        return NULL;
    }
    
    // Get the category total at the specified index
    CategoryTotal* category = &categoryTotals[index];
    
    // Allocate memory for the JSON string
    // Format: {"name":"Food","total":25.75}
    char* json = (char*)malloc(MAX_STRING_LENGTH * 2);
    if (json == NULL) {
        return NULL;
    }
    
    // Format the category total data as JSON
    sprintf(json, "{\"name\":\"%s\",\"total\":%.2f}", category->name, category->total);
    
    return json;
}

/**
 * Function to free memory allocated by getExpenseJSON or getCategoryTotalJSON
 * This function is exported to JavaScript to free memory allocated by C
 * 
 * @param ptr Pointer to the memory to free
 */
void EMSCRIPTEN_KEEPALIVE freeMemory(char* ptr) {
    free(ptr);
}

/**
 * Function to update the UI with the current expense data
 * This function calls JavaScript functions to update the HTML UI
 */
void updateUI() {
    // Call JavaScript function to update the expense table
    EM_ASM({
        // Call the JavaScript function to update the expense table
        updateExpenseTable();
    });
    
    // Call JavaScript function to update the total expenses display
    double total = calculateTotalExpenses();
    EM_ASM({
        // Call the JavaScript function to update the total expenses display
        updateTotalExpenses($0);
    }, total);
    
    // Call JavaScript function to update the category totals display
    EM_ASM({
        // Call the JavaScript function to update the category totals display
        updateCategoryTotals();
    });
}

/**
 * Function to add an expense from JavaScript
 * This function is exported to JavaScript to add a new expense
 * 
 * @param date The date of the expense
 * @param category The category of the expense
 * @param amount The amount of the expense
 * @param description The description of the expense
 * @return 1 if successful, 0 if the expenses array is full
 */
int EMSCRIPTEN_KEEPALIVE jsAddExpense(const char* date, const char* category, double amount, const char* description) {
    return addExpense(date, category, amount, description);
}

/**
 * Function to delete an expense from JavaScript
 * This function is exported to JavaScript to delete an expense
 * 
 * @param index The index of the expense to delete
 * @return 1 if successful, 0 if the index is invalid
 */
int EMSCRIPTEN_KEEPALIVE jsDeleteExpense(int index) {
    return deleteExpense(index);
}

/**
 * Function to clear all expenses from JavaScript
 * This function is exported to JavaScript to clear all expenses
 * 
 * @return 1 indicating success
 */
int EMSCRIPTEN_KEEPALIVE jsClearAllExpenses() {
    return clearAllExpenses();
}

/**
 * Function to get the total expenses from JavaScript
 * This function is exported to JavaScript to get the total expenses
 * 
 * @return The total amount of all expenses
 */
double EMSCRIPTEN_KEEPALIVE jsGetTotalExpenses() {
    return calculateTotalExpenses();
}

/**
 * Function to get the expense count from JavaScript
 * This function is exported to JavaScript to get the number of expenses
 * 
 * @return The number of expense entries
 */
int EMSCRIPTEN_KEEPALIVE jsGetExpenseCount() {
    return getExpenseCount();
}

/**
 * Function to get the category count from JavaScript
 * This function is exported to JavaScript to get the number of categories
 * 
 * @return The number of unique categories
 */
int EMSCRIPTEN_KEEPALIVE jsGetCategoryCount() {
    return getCategoryCount();
}

/**
 * Function to display a "Hello" alert message
 * This function is exported to JavaScript
 */
void EMSCRIPTEN_KEEPALIVE showHelloMessage() {
    EM_ASM({
        alert("Hello from C!");
    });
}

/**
 * Main function - entry point of the application
 * In a WebAssembly context, this function is called when the module is loaded
 */
int main() {
    printf("Budget Planner WebAssembly Application Initialized\n");
    
    // Initialize the expense count
    expenseCount = 0;
    
    // Initialize the category count
    categoryCount = 0;
    
    // Update the UI
    updateUI();
    
    return 0;
}
