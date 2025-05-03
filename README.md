# Data Synchronization Across Heterogeneous Systems

This project addresses the challenge of synchronizing data across diverse systems that may differ in platforms, data formats, and communication protocols. It provides a solution to ensure data consistency and integrity across such heterogeneous environments.

## 📁 Project Structure

```
Data-Synchronization-Across-Heterogeneous-Systems/
├── .idea/                   # IDE configuration files
├── src/                     # Java source code
├── .gitignore               # Specifies files to ignore in version control
├── pom.xml                  # Maven project configuration
└── Project_report.pdf       # Detailed project report
```

## 🛠️ Technologies Used

* **Programming Language:** Java
* **Build Tool:** Maven
* **Version Control:** Git

## 📄 Getting Started

### Prerequisites

* Java Development Kit (JDK) 8 or higher
* Maven 3.6 or higher
* Database servers (e.g., MySQL, PostgreSQL, MongoDB) installed and running

### Installation

1. **Clone the repository:**

   ```bash
   git clone https://github.com/subham-agarwal05/Data-Synchronization-Across-Heterogeneous-Systems.git
   cd Data-Synchronization-Across-Heterogeneous-Systems
   ```



2. **Build the project using Maven:**

   ```bash
   mvn clean install
   ```



### Database Configuration

Before running the application, ensure that all required database servers are up and running. Then, configure the database connection details in the respective DAO (Data Access Object) files:

1. **Start Database Servers:**

   * Ensure that all necessary database servers (e.g., MySQL, PostgreSQL, MongoDB) are installed and running on your system.

2. **Configure DAO Files:**

   * Navigate to the `src` directory and locate the DAO classes responsible for database interactions.

   * For each DAO class, update the database connection parameters (such as database name, username, and password) to match your local database setup.

   * Example (for a MySQL DAO):([GitHub][1])

     ```java
     String url = "jdbc:mysql://localhost:3306/your_database_name";
     String username = "your_username";
     String password = "your_password";
     ```

   * Repeat this process for each DAO class corresponding to different databases.

## 🚀 Usage

After configuring the DAO files and building the project, you can run the application by executing the `Main.java` file located in the `src` directory.

## 📘 Documentation

For a comprehensive understanding of the project's objectives, architecture, and implementation details, refer to the [Project\_report.pdf](Project_report.pdf) included in the repository.


---

*This README provides an overview of the project based on the available information. For more detailed insights, please refer to the project's source code and documentation.\**

---

