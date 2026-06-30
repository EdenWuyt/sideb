import { Pipe, PipeTransform } from '@angular/core';
import { ConfirmationToken } from '@stripe/stripe-js/dist/api/confirmation-tokens';

@Pipe({
  name: 'paymentMethod',
})
export class PaymentMethodPipe implements PipeTransform {
  transform(value?: ConfirmationToken['payment_method_preview'], ...args: unknown[]) {
    if (value?.card) {
      const {brand, last4, exp_month, exp_year} = value.card;
      return `${brand.toUpperCase()} **** **** **** ${last4}, EXP: ${exp_month.toString().padStart(2, '0')}/${exp_year}`;
    }
    return 'Unknown Payment Method';
  }
}
