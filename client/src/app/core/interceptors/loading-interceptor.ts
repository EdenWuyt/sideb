import { HttpInterceptorFn } from '@angular/common/http';
// import { delay } from 'rxjs/internal/operators/delay';
import { Busy } from '../services/busy';
import { inject } from '@angular/core';
import { finalize } from 'rxjs/internal/operators/finalize';

export const loadingInterceptor: HttpInterceptorFn = (req, next) => {
  const busyService = inject(Busy);
  busyService.busy();
  return next(req).pipe(
    // delay(500), // Simulate network delay for testing purposes
    finalize(() => busyService.idle())
  );
};
