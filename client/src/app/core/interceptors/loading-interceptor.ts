import { HttpInterceptorFn } from '@angular/common/http';
// import { delay } from 'rxjs/internal/operators/delay';
import { Busy } from '../services/busy';
import { inject } from '@angular/core';
import { finalize } from 'rxjs/internal/operators/finalize';
// import { environment } from '../../../environments/environment.development';
// import { identity } from 'rxjs/internal/util/identity';

export const loadingInterceptor: HttpInterceptorFn = (req, next) => {
  const busyService = inject(Busy);
  busyService.busy();
  return next(req).pipe(
    // (environment.production ? identity : delay(1000)),
    finalize(() => busyService.idle())
  );
};
