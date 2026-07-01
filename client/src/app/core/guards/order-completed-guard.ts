import { CanActivateFn, Router } from '@angular/router';
import { Order } from '../services/order';
import { inject } from '@angular/core/primitives/di';

export const orderCompletedGuard: CanActivateFn = (route, state) => {
  const orderService = inject(Order);
  const router = inject(Router);
  if (!orderService.orderCompleted()) {
    router.navigateByUrl('/shop');
    return false;
  }
  return true;
};
