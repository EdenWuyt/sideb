import { Pipe, PipeTransform } from '@angular/core';
import { ConfirmationToken } from '@stripe/stripe-js/dist/api/confirmation-tokens';

@Pipe({
  name: 'address',
})
export class AddressPipe implements PipeTransform {
  transform(value?: ConfirmationToken['shipping'], ...args: unknown[]) {
    if (value?.address && value.name) {
      const { line1, line2, city, state, postal_code, country } = value.address;
      return `${value.name}, ${line1}${line2 ? ', ' + line2 : ''}, ${city}, ${state}, ${postal_code}, ${country}`;
    }
    return 'Unknown Address';
  }
}
