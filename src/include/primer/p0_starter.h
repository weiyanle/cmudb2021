//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// p0_starter.h
//
// Identification: src/include/primer/p0_starter.h
//
// Copyright (c) 2015-2020, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <stdexcept>
#include <vector>

#include "common/exception.h"

namespace bustub {

/**
 * The Matrix type defines a common
 * interface for matrix operations.
 */
template <typename T>
class Matrix {
 protected:
  /**
   * TODO(P0): Add implementation
   *
   * Construct a new Matrix instance.
   * @param rows The number of rows
   * @param cols The number of columns
   *
   */
  Matrix(int rows, int cols) {
    linear_ = new T[rows * cols];
    this->rows_ = rows;
    this->cols_ = cols;
  }

  /** The number of rows in the matrix */
  int rows_;
  /** The number of columns in the matrix */
  int cols_;

  /**
   * TODO(P0): Allocate the array in the constructor.
   * TODO(P0): Deallocate the array in the destructor.
   * A flattened array containing the elements of the matrix.
   */
  T *linear_;

 public:
  /** @return The number of rows in the matrix */
  virtual int GetRowCount() const { return rows_; }

  /** @return The number of columns in the matrix */
  virtual int GetColumnCount() const { return cols_; }

  /**
   * Get the (i,j)th matrix element.
   *
   * Throw OUT_OF_RANGE if either index is out of range.
   *
   * @param i The row index
   * @param j The column index
   * @return The (i,j)th matrix element
   * @throws OUT_OF_RANGE if either index is out of range
   */
  virtual T GetElement(int i, int j) const {
    if (i < 0 || j < 0 || i >= this->rows_ || j >= this->cols_) {
      throw Exception(ExceptionType::OUT_OF_RANGE, "out of range!");
    }
    return *(linear_ + i * cols_ + j);
  }

  /**
   * Set the (i,j)th matrix element.
   *
   * Throw OUT_OF_RANGE if either index is out of range.
   *
   * @param i The row index
   * @param j The column index
   * @param val The value to insert
   * @throws OUT_OF_RANGE if either index is out of range
   */
  virtual void SetElement(int i, int j, T val) {
    if (i < 0 || j < 0 || i >= this->rows_ || j >= this->cols_) {
      throw Exception(ExceptionType::OUT_OF_RANGE, "out of range!");
    }
    *(linear_ + i * cols_ + j) = val;
  }

  /**
   * Fill the elements of the matrix from `source`.
   *
   * Throw OUT_OF_RANGE in the event that `source`
   * does not contain the required number of elements.
   *
   * @param source The source container
   * @throws OUT_OF_RANGE if `source` is incorrect size
   */
  virtual void FillFrom(const std::vector<T> &source) {
    if (static_cast<int>(source.size()) != rows_ * cols_) {
      throw Exception(ExceptionType::OUT_OF_RANGE, "incorrect size!");
    }
    for (int i = 0; i < rows_ * cols_; ++i) {
      *(linear_ + i) = source[i];
    }
  }

  /**
   * Destroy a matrix instance.
   * TODO(P0): Add implementation
   */
  virtual ~Matrix() {
    free(linear_);
    linear_ = nullptr;
  }
};

/**
 * The RowMatrix type is a concrete matrix implementation.
 * It implements the interface defined by the Matrix type.
 */
template <typename T>
class RowMatrix : public Matrix<T> {
 public:
  /**
   * TODO(P0): Add implementation
   *
   * Construct a new RowMatrix instance.
   * @param rows The number of rows
   * @param cols The number of columns
   */
  RowMatrix(int rows, int cols) : Matrix<T>(rows, cols) {
    data_ = new T *[rows];
    for (int i = 0; i < rows; i++) {
      data_[i] = new T[cols];
    }
  }

  /**
   * TODO(P0): Add implementation
   * @return The number of rows in the matrix
   */
  int GetRowCount() const override { return Matrix<T>::rows_; }

  /**
   * TODO(P0): Add implementation
   * @return The number of columns in the matrix
   */
  int GetColumnCount() const override { return Matrix<T>::cols_; }

  /**
   * TODO(P0): Add implementation
   *
   * Get the (i,j)th matrix element.
   *
   * Throw OUT_OF_RANGE if either index is out of range.
   *
   * @param i The row index
   * @param j The column index
   * @return The (i,j)th matrix element
   * @throws OUT_OF_RANGE if either index is out of range
   */
  T GetElement(int i, int j) const override {
    if (i < 0 || j < 0 || i >= this->rows_ || j >= this->cols_) {
      throw Exception(ExceptionType::OUT_OF_RANGE, "out of range!");
    }
    return data_[i][j];
  }

  /**
   * Set the (i,j)th matrix element.
   *
   * Throw OUT_OF_RANGE if either index is out of range.
   *
   * @param i The row index
   * @param j The column index
   * @param val The value to insert
   * @throws OUT_OF_RANGE if either index is out of range
   */
  void SetElement(int i, int j, T val) override {
    if (i < 0 || j < 0 || i >= this->rows_ || j >= this->cols_) {
      throw Exception(ExceptionType::OUT_OF_RANGE, "out of range!");
    }
    data_[i][j] = val;
  }

  /**
   * TODO(P0): Add implementation
   *
   * Fill the elements of the matrix from `source`.
   *
   * Throw OUT_OF_RANGE in the event that `source`
   * does not contain the required number of elements.
   *
   * @param source The source container
   * @throws OUT_OF_RANGE if `source` is incorrect size
   */
  void FillFrom(const std::vector<T> &source) override {
    if (static_cast<int>(source.size()) != Matrix<T>::rows_ * Matrix<T>::cols_) {
      throw Exception(ExceptionType::OUT_OF_RANGE, "incorrct size!");
    }
    for (int i = 0; i < Matrix<T>::rows_; i++) {
      for (int j = 0; j < Matrix<T>::cols_; j++) {
        data_[i][j] = source[i * Matrix<T>::cols_ + j];
      }
    }
  }

  /**
   * TODO(P0): Add implementation
   *
   * Destroy a RowMatrix instance.
   */
  ~RowMatrix() override {
    delete[] Matrix<T>::linear_;
    Matrix<T>::linear_ = nullptr;
    for (int i = 0; i < Matrix<T>::rows_; i++) {
      delete[] data_[i];
    }
    delete[] data_;
    data_ = nullptr;
  };

 private:
  /**
   * A 2D array containing the elements of the matrix in row-major format.
   *
   * TODO(P0):
   * - Allocate the array of row pointers in the constructor.
   * - Use these pointers to point to corresponding elements of the `linear` array.
   * - Don't forget to deallocate the array in the destructor.
   */
  T **data_;
};

/**
 * The RowMatrixOperations class defines operations
 * that may be performed on instances of `RowMatrix`.
 */
template <typename T>
class RowMatrixOperations {
 public:
  /**
   * Compute (`matrixA` + `matrixB`) and return the result.
   * Return `nullptr` if dimensions mismatch for input matrices.
   * @param matrixA Input matrix
   * @param matrixB Input matrix
   * @return The result of matrix addition
   */
  static std::unique_ptr<RowMatrix<T>> Add(const RowMatrix<T> *matrixA, const RowMatrix<T> *matrixB) {
    // TODO(P0): Add implementation
    int r = matrixA->GetRowCount();
    int c = matrixA->GetColumnCount();
    if (matrixB->GetRowCount() == r && matrixB->GetColumnCount() == c) {
      std::unique_ptr<RowMatrix<T>> ret(new RowMatrix<T>(r, c));
      for (int i = 0; i < r; i++) {
        for (int j = 0; j < c; j++) {
          ret->SetElement(i, j, matrixA->GetElement(i, j) + matrixB->GetElement(i, j));
        }
      }
      return ret;
    }
    return nullptr;
  }

  /**
   * Compute the matrix multiplication (`matrixA` * `matrixB` and return the result.
   * Return `nullptr` if dimensions mismatch for input matrices.
   * @param matrixA Input matrix
   * @param matrixB Input matrix
   * @return The result of matrix multiplication
   */
  static std::unique_ptr<RowMatrix<T>> Multiply(const RowMatrix<T> *matrixA, const RowMatrix<T> *matrixB) {
    // TODO(P0): Add implementation
    int r1 = matrixA->GetRowCount();
    int c1 = matrixA->GetColumnCount();
    int r2 = matrixB->GetRowCount();
    int c2 = matrixB->GetColumnCount();
    if (c1 == r2) {
      std::unique_ptr<RowMatrix<T>> ret(new RowMatrix<T>(r1, c2));
      for (int i = 0; i < r1; i++) {
        for (int j = 0; j < c2; j++) {
          ret->SetElement(i, j, 0);
          for (int k = 0; k < c1; k++) {
            ret->SetElement(i, j, ret->GetElement(i, j) + matrixA->GetElement(i, k) * matrixB->GetElement(k, j));
          }
        }
      }
      return ret;
    }
    return nullptr;
  }

  /**
   * Simplified General Matrix Multiply operation. Compute (`matrixA` * `matrixB` + `matrixC`).
   * Return `nullptr` if dimensions mismatch for input matrices.
   * @param matrixA Input matrix
   * @param matrixB Input matrix
   * @param matrixC Input matrix
   * @return The result of general matrix multiply
   */
  static std::unique_ptr<RowMatrix<T>> GEMM(const RowMatrix<T> *matrixA, const RowMatrix<T> *matrixB,
                                            const RowMatrix<T> *matrixC) {
    // TODO(P0): Add implementation
    int r1 = matrixA->GetRowCount();
    int c1 = matrixA->GetColumnCount();
    int r2 = matrixB->GetRowCount();
    int c2 = matrixB->GetColumnCount();
    int r3 = matrixC->GetRowCount();
    int c3 = matrixC->GetColumnCount();
    if (c1 == r2 && r1 == r3 && c2 == c3) {
      return Add((Multiply(matrixA, matrixB)).get(), matrixC);
    }
    return nullptr;
  }
};
}  // namespace bustub
