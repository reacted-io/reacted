/*
 * Copyright (c) 2021 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.flow.operators.exceptions;

public class OperatorInitException extends RuntimeException {
  public OperatorInitException(String message) { super(message); }
  public OperatorInitException(String message, Throwable cause) {
    super(message, cause);
  }
}
