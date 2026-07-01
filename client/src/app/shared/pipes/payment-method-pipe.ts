import { Pipe, PipeTransform } from '@angular/core';
import { ConfirmationToken } from '@stripe/stripe-js/dist/api/confirmation-tokens';
import { PaymentSummary } from '../models/order';

@Pipe({
  name: 'paymentMethod',
})
export class PaymentMethodPipe implements PipeTransform {
  transform(value?: ConfirmationToken['payment_method_preview'] | PaymentSummary, ...args: unknown[]) {
    if (value && 'card' in value) {
      const {brand, last4, exp_month, exp_year} = (value as ConfirmationToken['payment_method_preview']).card!;
      return `${brand.toUpperCase()} **** **** **** ${last4}, EXP: ${exp_month.toString().padStart(2, '0')}/${exp_year}`;
    } else if (value && 'brand' in value) {
      const {brand, last4Digits, expMonth, expYear} = value as PaymentSummary;
      return `${brand.toUpperCase()} **** **** **** ${last4Digits}, EXP: ${expMonth.toString().padStart(2, '0')}/${expYear}`;
    } else {
      return 'Unknown Payment Method';
    }
  }
}
